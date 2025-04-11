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
use DateTime;

#[AsCommand(
  name: 'app:db-list',
  description: 'List all S3 buckets',
)]
class DbList extends Command
{
  // Database path prefix to filter objects
  private const DB_PATH_PREFIX = 'db/';

  protected function configure(): void
  {
    $this
      ->addOption('region', 'r', InputOption::VALUE_OPTIONAL, 'AWS region', 'us-east-1')
      ->addOption('s3profile', 'p', InputOption::VALUE_OPTIONAL, 'AWS profile', 'default')
      ->addOption('bucket', 'b', InputOption::VALUE_OPTIONAL, 'List objects in specific bucket')
      ->addOption('max-keys', 'm', InputOption::VALUE_OPTIONAL, 'Maximum number of objects to list', 25)
      ->addOption('prefix', null, InputOption::VALUE_OPTIONAL, 'Filter objects by prefix/path')
      ->addOption('date', 'd', InputOption::VALUE_OPTIONAL, 'Filter objects by date (YYYY-MM-DD), defaults to today')
      ->addOption('no-date', null, InputOption::VALUE_NONE, 'Disable automatic date filtering')
      ->addOption('all', 'a', InputOption::VALUE_NONE, 'Show all objects, not just database files');
  }

  protected function execute(InputInterface $input, OutputInterface $output): int
  {
    $io = new SymfonyStyle($input, $output);
    $region = $input->getOption('region');
    $profile = $input->getOption('s3profile');
    $bucketName = $input->getOption('bucket');
    $maxKeys = (int)$input->getOption('max-keys');
    $prefix = $input->getOption('prefix');
    $date = $input->getOption('date');
    $noDate = $input->getOption('no-date');
    $showAll = $input->getOption('all');

    // Handle date prefix logic
    if ($bucketName && !$noDate) {
      // If date is not provided but --no-date is not set, use today's date
      if ($date === null) {
        $date = (new DateTime())->format('Y-m-d');
      }

      // Validate date format
      if (!$this->isValidDate($date)) {
        $io->error('Invalid date format. Please use YYYY-MM-DD format.');
        return Command::FAILURE;
      }

      // Combine date with existing prefix if any
      if ($prefix) {
        $prefix = $prefix . '/' . $date;
      } else {
        $prefix = $date;
      }
    }

    // Add db/ prefix if not showing all files and no specific prefix is set
    if ($bucketName && !$showAll && !$prefix) {
      $prefix = self::DB_PATH_PREFIX;
    } else if ($bucketName && !$showAll && $prefix) {
      // If we have a prefix but still want to filter for db files, 
      // we need to check this later when processing results
      $filterForDbFiles = true;
    }

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
        $this->listBucketObjects($s3Client, $io, $bucketName, $maxKeys, $prefix, $showAll);
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

  private function listBucketObjects(S3Client $s3Client, SymfonyStyle $io, string $bucketName, int $maxKeys, ?string $prefix = null, bool $showAll = false): void
  {
    $prefixInfo = $prefix ? sprintf(' with prefix "%s"', $prefix) : '';
    $dbFilterInfo = !$showAll ? ' (database files only)' : '';
    
    $io->section(sprintf('Listing objects in bucket: %s%s%s (max: %d objects)', 
      $bucketName, $prefixInfo, $dbFilterInfo, $maxKeys));

    // Check if bucket exists
    if (!$s3Client->doesBucketExist($bucketName)) {
      $io->error(sprintf('Bucket "%s" does not exist.', $bucketName));
      return;
    }

    // Prepare parameters for listing objects
    $params = [
      'Bucket' => $bucketName,
      'MaxKeys' => $maxKeys
    ];

    // Add prefix if provided
    if ($prefix) {
      $params['Prefix'] = $prefix;
    }

    // List objects in the bucket
    $result = $s3Client->listObjects($params);

    $objects = $result['Contents'] ?? [];

    if (count($objects) > 0) {
      $tableRows = [];
      $filteredCount = 0;
      
      foreach ($objects as $object) {
        // Skip non-database files if we're not showing all files
        // and we haven't already filtered with the prefix
        if (!$showAll && !$this->isDbFile($object['Key']) && 
            (!$prefix || strpos($prefix, self::DB_PATH_PREFIX) !== 0)) {
          $filteredCount++;
          continue;
        }
        
        $size = $this->formatSize($object['Size']);
        $tableRows[] = [
          $object['Key'],
          $size,
          $object['LastModified']->format('Y-m-d H:i:s')
        ];
      }

      if (count($tableRows) > 0) {
        $io->table(['Key', 'Size', 'Last Modified'], $tableRows);
        
        if ($filteredCount > 0) {
          $io->note(sprintf('Filtered out %d non-database files. Use --all to show all files.', $filteredCount));
        }
        
        if (isset($result['IsTruncated']) && $result['IsTruncated']) {
          $io->note(sprintf('Showing only %d objects. Use --max-keys option to show more.', $maxKeys));
        }
      } else {
        $io->warning(sprintf('No database files found in bucket "%s"%s.', $bucketName, $prefixInfo));
      }
    } else {
      $io->warning(sprintf('No objects found in bucket "%s"%s.', $bucketName, $prefixInfo));
    }
  }

  private function isDbFile(string $key): bool
  {
    // Check if the key contains the database path prefix
    return strpos($key, self::DB_PATH_PREFIX) !== false;
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

  private function isValidDate(string $date): bool
  {
    $format = 'Y-m-d';
    $dateTime = DateTime::createFromFormat($format, $date);
    return $dateTime && $dateTime->format($format) === $date;
  }
}