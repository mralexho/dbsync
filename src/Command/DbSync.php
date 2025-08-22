<?php

namespace App\Command;

use Aws\S3\S3Client;
use Aws\Exception\AwsException;
use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(
  name: 'app:db-sync',
  description: 'Synchronize the database with the latest version from S3',
)]
class DbSync extends Command
{
  protected function configure(): void
  {
    $this
      ->addOption('region', 'r', InputOption::VALUE_OPTIONAL, 'AWS region', 'us-east-1')
      ->addOption('s3profile', 'p', InputOption::VALUE_OPTIONAL, 'AWS profile', 'default')
      ->addOption('bucket', 'b', InputOption::VALUE_OPTIONAL, 'List objects in specific bucket')
      ->addOption('max-keys', 'm', InputOption::VALUE_OPTIONAL, 'Maximum number of objects to list', 25)
      ->addOption('download-dir', 'd', InputOption::VALUE_OPTIONAL, 'Directory to save downloaded files', getcwd());
  }

  protected function execute(InputInterface $input, OutputInterface $output): int
  {
    $io = new SymfonyStyle($input, $output);
    $region = $input->getOption('region');
    $profile = $input->getOption('s3profile');
    $bucketName = $input->getOption('bucket');
    $maxKeys = (int)$input->getOption('max-keys');
    $downloadDir = $input->getOption('download-dir');

    $io->title('Connecting to AWS S3');
    $io->text(sprintf('Using region: %s and profile: %s', $region, $profile));

    try {
      // Create an S3 client
      $s3Client = new S3Client([
        'version' => 'latest',
        'region'  => $region,
        'profile' => $profile
      ]);

      // If bucket name is provided, list objects in that bucket
      if ($bucketName) {
        $this->listBucketObjects($s3Client, $io, $bucketName, $maxKeys, $downloadDir);
      } else {
        // List all buckets
        $this->listAllBuckets($s3Client, $io);
      }

      return Command::SUCCESS;
    } catch (AwsException $e) {
      $io->error('AWS Error: ' . $e->getMessage());
      return Command::FAILURE;
    } catch (\Exception $e) {
      $io->error('Error: ' . $e->getMessage());
      return Command::FAILURE;
    }
  }

  private function listAllBuckets(S3Client $s3Client, SymfonyStyle $io): void
  {
    // List all buckets
    $result = $s3Client->listBuckets();
    $buckets = $result['Buckets'];

    if (count($buckets) > 0) {
      $io->success('Found the following S3 buckets:');

      $tableRows = [];
      foreach ($buckets as $bucket) {
        $tableRows[] = [
          $bucket['Name'],
          $bucket['CreationDate']->format('Y-m-d H:i:s')
        ];
      }

      $io->table(['Bucket Name', 'Creation Date'], $tableRows);
    } else {
      $io->warning('No S3 buckets found in your account.');
    }
  }

  private function listBucketObjects(S3Client $s3Client, SymfonyStyle $io, string $bucketName, int $maxKeys, string $downloadDir): void
  {
    $io->section(sprintf('Listing objects in bucket: %s (max: %d objects)', $bucketName, $maxKeys));

    // Check if bucket exists
    if (!$s3Client->doesBucketExist($bucketName)) {
      $io->error(sprintf('Bucket "%s" does not exist.', $bucketName));
      return;
    }

    // Get a list of all date-based folders first
    $result = $s3Client->listObjectsV2([
      'Bucket' => $bucketName,
      'Delimiter' => '/',
      'MaxKeys' => 1000 // Get a reasonable number of prefixes
    ]);

    $prefixes = $result['CommonPrefixes'] ?? [];
    $dateFolders = [];
    
    // Extract date folders that match YYYY-MM-DD/ pattern
    foreach ($prefixes as $prefix) {
      $prefixName = $prefix['Prefix'];
      if (preg_match('/^\d{4}-\d{2}-\d{2}\//', $prefixName)) {
        $dateFolders[] = $prefixName;
      }
    }
    
    if (empty($dateFolders)) {
      $io->warning(sprintf('No date-based folders (YYYY-MM-DD/) found in bucket "%s".', $bucketName));
      return;
    }
    
    // Sort date folders in descending order (newest first)
    rsort($dateFolders);
    
    $allDbObjects = [];
    $totalFound = 0;
    
    // For each date folder, check if it has a db/ subfolder and list its contents
    foreach ($dateFolders as $dateFolder) {
      $dbPrefix = $dateFolder . 'db/';
      
      $dbResult = $s3Client->listObjectsV2([
        'Bucket' => $bucketName,
        'Prefix' => $dbPrefix,
        'MaxKeys' => $maxKeys
      ]);
      
      $dbObjects = $dbResult['Contents'] ?? [];
      
      if (!empty($dbObjects)) {
        $totalFound += count($dbObjects);
        $allDbObjects = array_merge($allDbObjects, $dbObjects);
        
        // If we've collected enough objects, stop searching more folders
        if (count($allDbObjects) >= $maxKeys) {
          $allDbObjects = array_slice($allDbObjects, 0, $maxKeys);
          break;
        }
      }
    }
    
    if (count($allDbObjects) > 0) {
      $tableRows = [];
      $fileChoices = [];
      $index = 1;
      
      foreach ($allDbObjects as $object) {
        $size = $this->formatSize($object['Size']);
        $tableRows[] = [
          $index,
          $object['Key'],
          $size,
          $object['LastModified']->format('Y-m-d H:i:s')
        ];
        
        $fileChoices[$index] = $object['Key'];
        $index++;
      }

      $io->table(['#', 'Key', 'Size', 'Last Modified'], $tableRows);
      
      $io->info(sprintf('Found %d objects in YYYY-MM-DD/db/ folders (showing %d).', 
        $totalFound, count($allDbObjects)));
        
      if ($totalFound > $maxKeys) {
        $io->note(sprintf('Showing only %d objects. Use --max-keys option to show more.', $maxKeys));
      }
      
      // Ask user to select a file to download directly
      $selectedIndex = $io->ask(
        'Enter the number (#) of the file you want to download (or press Enter to skip)',
        null,
        function ($answer) use ($fileChoices) {
          if ($answer === null || $answer === '') {
            return null;
          }
          $answer = (int)$answer;
          if (!isset($fileChoices[$answer])) {
            throw new \RuntimeException('Invalid selection. Please enter a valid number from the list.');
          }
          return $answer;
        }
      );
      
      if ($selectedIndex !== null) {
        $selectedFile = $fileChoices[$selectedIndex];
        $io->success(sprintf('Selected file: %s', $selectedFile));
        $this->downloadFile($s3Client, $io, $bucketName, $selectedFile, $downloadDir);
      }
    } else {
      $io->warning(sprintf('No objects found in YYYY-MM-DD/db/ folders in bucket "%s".', $bucketName));
    }
  }

  private function formatSize(int $sizeInBytes): string
  {
    $units = ['B', 'KB', 'MB', 'GB', 'TB'];
    $size = $sizeInBytes;
    $unitIndex = 0;

    while ($size >= 1024 && $unitIndex < count($units) - 1) {
      $size /= 1024;
      $unitIndex++;
    }

    return round($size, 2) . ' ' . $units[$unitIndex];
  }

  /**
   * Sync a specific file from S3
   */
  private function downloadFile(S3Client $s3Client, SymfonyStyle $io, string $bucketName, string $objectKey, string $downloadDir): void
  {
    $io->section('File Download');
    
    try {
      // Ensure the download directory exists
      if (!is_dir($downloadDir)) {
        if (!mkdir($downloadDir, 0755, true)) {
          throw new \RuntimeException(sprintf('Could not create download directory: %s', $downloadDir));
        }
      }
      
      // Create a filename for the downloaded file
      $filename = basename($objectKey);
      $filePath = $downloadDir . '/' . $filename;
      
      $io->text(sprintf('Downloading %s to %s...', $objectKey, $filePath));
      
      // Download the file from S3
      $s3Client->getObject([
        'Bucket' => $bucketName,
        'Key' => $objectKey,
        'SaveAs' => $filePath
      ]);
      
      $io->success(sprintf('File successfully downloaded to: %s', $filePath));
      
    } catch (\Exception $e) {
      $io->error('Error during download: ' . $e->getMessage());
    }
  }
  
  /**
   * Execute a shell command and return the output
   */
  private function executeCommand(SymfonyStyle $io, string $command): bool
  {
    $io->text(sprintf('Executing: %s', $command));
    
    $process = proc_open(
      $command,
      [
        0 => ['pipe', 'r'],  // stdin
        1 => ['pipe', 'w'],  // stdout
        2 => ['pipe', 'w'],  // stderr
      ],
      $pipes
    );
    
    if (is_resource($process)) {
      // Close stdin
      fclose($pipes[0]);
      
      // Read stdout
      $output = stream_get_contents($pipes[1]);
      fclose($pipes[1]);
      
      // Read stderr
      $error = stream_get_contents($pipes[2]);
      fclose($pipes[2]);
      
      // Close process
      $exitCode = proc_close($process);
      
      if ($exitCode === 0) {
        if (!empty($output)) {
          $io->text('Command output:');
          $io->writeln($output);
        }
        return true;
      } else {
        $io->error(sprintf('Command failed with exit code %d', $exitCode));
        if (!empty($error)) {
          $io->text('Error output:');
          $io->writeln($error);
        }
        return false;
      }
    }
    
    $io->error('Failed to execute command');
    return false;
  }
}
