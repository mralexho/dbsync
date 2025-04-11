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

    // List objects in the bucket
    $result = $s3Client->listObjects([
      'Bucket' => $bucketName,
      'MaxKeys' => $maxKeys
    ]);

    $objects = $result['Contents'] ?? [];

    if (count($objects) > 0) {
      $tableRows = [];
      foreach ($objects as $object) {
        $size = $this->formatSize($object['Size']);
        $tableRows[] = [
          $object['Key'],
          $size,
          $object['LastModified']->format('Y-m-d H:i:s')
        ];
      }

      $io->table(['Key', 'Size', 'Last Modified'], $tableRows);

      if (isset($result['IsTruncated']) && $result['IsTruncated']) {
        $io->note(sprintf('Showing only %d objects. Use --max-keys option to show more.', $maxKeys));
      }
    } else {
      $io->warning(sprintf('No objects found in bucket "%s".', $bucketName));
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
}