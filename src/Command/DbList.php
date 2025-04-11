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
  name: 'app:db-list',
  description: 'List all S3 buckets',
)]
class DbList extends Command
{
  protected function configure(): void
  {
    $this
      ->addOption('region', 'r', InputOption::VALUE_OPTIONAL, 'AWS region', 'us-east-1')
      ->addOption('s3profile', 'p', InputOption::VALUE_OPTIONAL, 'AWS profile', 'default')
      ->addOption('bucket', 'b', InputOption::VALUE_OPTIONAL, 'List objects in specific bucket')
      ->addOption('max-keys', 'm', InputOption::VALUE_OPTIONAL, 'Maximum number of objects to list', 25);
  }

  protected function execute(InputInterface $input, OutputInterface $output): int
  {
    $io = new SymfonyStyle($input, $output);
    $region = $input->getOption('region');
    $profile = $input->getOption('s3profile');
    $bucketName = $input->getOption('bucket');
    $maxKeys = (int)$input->getOption('max-keys');

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
        $this->listBucketObjects($s3Client, $io, $bucketName, $maxKeys);
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

  private function listBucketObjects(S3Client $s3Client, SymfonyStyle $io, string $bucketName, int $maxKeys): void
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
      
      // Ask user to select a file to sync directly
      $selectedIndex = $io->ask(
        'Enter the number (#) of the file you want to sync (or press Enter to skip)',
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
        
        // Ask for confirmation before proceeding
        if ($io->confirm(sprintf('Are you sure you want to sync %s?', $selectedFile), false)) {
          $io->text('Starting sync process...');
          $this->syncFile($s3Client, $io, $bucketName, $selectedFile);
        } else {
          $io->text('Sync cancelled.');
        }
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
  private function syncFile(S3Client $s3Client, SymfonyStyle $io, string $bucketName, string $objectKey): void
  {
    $io->section('File Sync');
    
    try {
      // Get the file extension to determine file type
      $extension = pathinfo($objectKey, PATHINFO_EXTENSION);
      
      // Create a temporary file to download to
      $tempFile = tempnam(sys_get_temp_dir(), 'db_sync_');
      
      $io->text(sprintf('Downloading %s to temporary file...', $objectKey));
      
      // Download the file from S3
      $s3Client->getObject([
        'Bucket' => $bucketName,
        'Key' => $objectKey,
        'SaveAs' => $tempFile
      ]);
      
      $io->text('Download complete. Processing file...');
      
      // Handle different file types
      switch (strtolower($extension)) {
        case 'sql':
          $this->importSqlFile($io, $tempFile);
          break;
        case 'gz':
        case 'gzip':
          $this->importGzipFile($io, $tempFile);
          break;
        case 'zip':
          $this->importZipFile($io, $tempFile);
          break;
        default:
          $io->error(sprintf('Unsupported file type: %s', $extension));
      }
      
      // Clean up the temporary file
      if (file_exists($tempFile)) {
        unlink($tempFile);
        $io->text('Temporary file removed.');
      }
      
    } catch (\Exception $e) {
      $io->error('Error during sync: ' . $e->getMessage());
    }
  }
  
  /**
   * Import a SQL file
   */
  private function importSqlFile(SymfonyStyle $io, string $filePath): void
  {
    $io->text('Processing SQL file...');
    $io->note('This is a placeholder. Implement actual SQL import logic here.');
    // TODO: Implement actual SQL import logic
    $io->success('SQL file processed successfully.');
  }
  
  /**
   * Import a gzipped file
   */
  private function importGzipFile(SymfonyStyle $io, string $filePath): void
  {
    $io->text('Processing gzipped file...');
    
    // Create a temporary file for the uncompressed content
    $tempSqlFile = tempnam(sys_get_temp_dir(), 'db_sync_sql_');
    
    try {
      // Uncompress the gzip file
      $gzipHandle = gzopen($filePath, 'rb');
      $sqlHandle = fopen($tempSqlFile, 'wb');
      
      while (!gzeof($gzipHandle)) {
        fwrite($sqlHandle, gzread($gzipHandle, 4096));
      }
      
      gzclose($gzipHandle);
      fclose($sqlHandle);
      
      // Process the uncompressed SQL file
      $this->importSqlFile($io, $tempSqlFile);
      
      // Clean up
      if (file_exists($tempSqlFile)) {
        unlink($tempSqlFile);
      }
      
    } catch (\Exception $e) {
      $io->error('Error processing gzipped file: ' . $e->getMessage());
      
      // Clean up on error
      if (file_exists($tempSqlFile)) {
        unlink($tempSqlFile);
      }
    }
  }
  
  /**
   * Import a ZIP file
   */
  private function importZipFile(SymfonyStyle $io, string $filePath): void
  {
    $io->text('Processing ZIP file...');
    
    try {
      $zip = new \ZipArchive();
      if ($zip->open($filePath) === true) {
        // Create a temporary directory for extraction
        $tempDir = sys_get_temp_dir() . '/db_sync_zip_' . uniqid();
        mkdir($tempDir);
        
        // Extract the ZIP file
        $zip->extractTo($tempDir);
        $zip->close();
        
        $io->text('ZIP file extracted. Looking for SQL files...');
        
        // Find SQL files in the extracted directory
        $sqlFiles = glob($tempDir . '/*.sql');
        
        if (count($sqlFiles) > 0) {
          foreach ($sqlFiles as $sqlFile) {
            $io->text(sprintf('Found SQL file: %s', basename($sqlFile)));
            $this->importSqlFile($io, $sqlFile);
          }
        } else {
          $io->warning('No SQL files found in the ZIP archive.');
        }
        
        // Clean up the temporary directory
        $this->removeDirectory($tempDir);
        
      } else {
        $io->error('Failed to open ZIP file.');
      }
    } catch (\Exception $e) {
      $io->error('Error processing ZIP file: ' . $e->getMessage());
      
      // Clean up on error
      if (isset($tempDir) && is_dir($tempDir)) {
        $this->removeDirectory($tempDir);
      }
    }
  }
  
  /**
   * Recursively remove a directory and its contents
   */
  private function removeDirectory(string $dir): void
  {
    if (is_dir($dir)) {
      $objects = scandir($dir);
      foreach ($objects as $object) {
        if ($object != "." && $object != "..") {
          if (is_dir($dir . "/" . $object)) {
            $this->removeDirectory($dir . "/" . $object);
          } else {
            unlink($dir . "/" . $object);
          }
        }
      }
      rmdir($dir);
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