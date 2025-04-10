<?php

namespace App\Command;

use Symfony\Component\Console\Attribute\AsCommand;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;
use Symfony\Component\Console\Style\SymfonyStyle;

#[AsCommand(
    name: 'app:db-sync',
    description: 'Syncronize the database with the latest version from S3',
)]
class DbSync extends Command
{
    protected function configure(): void
    {
        $this
            ->addArgument('bucket', InputArgument::REQUIRED, 'S3 bucket name');
    }

    protected function execute(InputInterface $input, OutputInterface $output): int
    {
        $io = new SymfonyStyle($input, $output);
        $bucket = $input->getArgument('bucket');
        $message = sprintf('Syncing the latest database from S3 bucket: %s', $bucket);
        
        $io->success($message);

        return Command::SUCCESS;
    }
}