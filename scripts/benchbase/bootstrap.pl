#!/usr/bin/perl
use v5.32;

use utf8;
use warnings;

use POSIX qw/:sys_wait_h/;

use File::Path qw/make_path/;
use IO::File;
use Cwd;
use IPC::Open2;

use Term::ANSIColor;
use Carp;

local $| = 1;

sub usage { qq($0 - bootstraps ultraverse testcase

BASIC USAGE
    $0 [benchmark] [txn-amount]

SYNOPSIS
    export ULTRAVERSE_HOME=\$HOME/ultraverse/build/release
    export BENCHBASE_HOME=\$HOME/benchbase

    $0 epinions 1m

    cd runs/1234-epinions-1m
    
    # set environment variables
    vi envvars
    
    # set key-columns, ... etc
    vi 01-create-cluster.sh
    vi 02-testcase-main.sh


    # run isolated MariaDB instance
    docker compose up -d

    # create statelog
    ./prepare-create-statelog.sh
    # create cluster
    ./01-create-cluster.sh
    # run test case
    ./02-testcase-main.sh

    # stop isolated MariaDB instance
    docker compose stop


ENVIRONMENT VARIABLES
    ULTRAVERSE_HOME     path to ultraverse binaries
    BENCHBASE_HOME      path to benchbase scripts
)}

sub docker_compose {
    my $project_directory = shift @_;

    my $pid = open2(
        my $stdout, undef,
        'docker', 'compose', '--project-directory', $project_directory, 
        @_
    );

    while (waitpid($pid, WNOHANG) == 0) {
        my $line = <$stdout>;
        chomp $line;
        printf("\33[2K\r");
        printf("%s", $line) if $line;
    }

    printf("\n");

    return $? >> 8;
}

sub benchbase {
    my $config = shift @_;
    my $benchbase_path = $config->{'path'}->{'benchbase'};

    my $cwd = getcwd;
    chdir $benchbase_path;


    my $pid = open2(
        my $stdout, undef,
        './run-mariadb', @_
    );


    while (waitpid($pid, WNOHANG) == 0) {
        my $line = <$stdout>;
        print($line) if $line;
    }

    printf("\n");

    my $retcode = ${^CHILD_ERROR_NATIVE} >> 8;

    chdir $cwd;
    return $retcode;
}

sub ultraverse {
    my $config = shift @_;
    my $ultraverse_path = $config->{'path'}->{'ultraverse'};
    my $binname = shift @_;
    my $logname = shift @_;

    my $cwd = getcwd;

    my $fh_log = IO::File->new(
        $logname,
        'w'
    );

    my $pid = open2(
        my $stdout, undef,
        $binname, @_
    );


    while (waitpid($pid, WNOHANG) == 0) {
        my $line = <$stdout>;
        if ($line) {
            print $line;
            print $fh_log $line;
        }
    }

    printf("\n");

    my $retcode = ${^CHILD_ERROR_NATIVE} >> 8;

    chdir $cwd;
    return $retcode;
}

sub mysqldump {
    my $dbname = shift @_;
    my $dumpto = shift @_;

    my $fh_dbdump = IO::File->new(
        $dumpto,
        'w'
    );

    my $pid = open2(
        my $stdout, '<&STDIN',
        'mysqldump', 
        '-R', # include procedures
        '-h', '127.0.0.1',
        '-u', 'admin',
        '--password=password',
        $dbname
    );

    while (waitpid($pid, WNOHANG) == 0) {
        my $line = <$stdout>;
        if ($line) {
            $line =~ s/DEFINER=[^\s]+//g;
            print $fh_dbdump $line;
        }
    }

    my $retcode = ${^CHILD_ERROR_NATIVE} >> 8;

    return $retcode;
}




sub bootstrap {
    my ($benchmark, $txn_amount, $config) = @_;

    my $cwd = getcwd;
    my $project_path = sprintf("runs/%d-%s-%s", time, $benchmark, $txn_amount);
    
    say colored("starting benchmark ", "bold white"), 
        colored($benchmark, "bold blue"), 
        colored(" for $txn_amount", "bold white");

    system('cp', '-r', 'base', $project_path);

    my $fh_report = IO::File->new(
        sprintf("%s/report.txt", $project_path),
        'w'
    );


    say colored("starting ", "bold white"), 
        colored("MariaDB", "bold blue"); 

    if (docker_compose($project_path, 'up', '-d') != 0) {
        croak("failed to start MariaDB");
    }

    $SIG{INT} = sub {
        chdir $cwd;

        say colored("stopping ", "bold white"), 
            colored("MariaDB", "bold blue"); 

        if (docker_compose($project_path, 'down') != 0) {
            croak("failed to stop MariaDB");
        }

        exit 1;
    };

    sleep 5;

    eval {
        say colored("preparing ", "bold white"), 
            colored("benchbase", "bold blue"); 

        if (benchbase($config, $benchmark, 'mariadb', $txn_amount, 'prepare') != 0) {
            croak("failed to run benchbase");
        }

        if (mysqldump('benchbase', sprintf("%s/dbdump.sql", $project_path)) != 0) {
            croak("failed to execute mysqldump");
        }

        if (docker_compose($project_path, 'down') != 0) {
            croak("failed to stop MariaDB");
        }

        say colored("flushing ", "bold white"), 
            colored("Binary Log", "bold blue"); 

        system(
            'sudo', 'sh', '-c',
            "rm -rf $project_path/db_data/server-binlog*"
        );

        if (docker_compose($project_path, 'up', '-d') != 0) {
            croak("failed to start MariaDB");
        }

        sleep 5;

        say colored("starting ", "bold white"), 
            colored("benchbase", "bold blue"); 

        my $retcode = benchbase($config, $benchmark, 'mariadb', $txn_amount, 'execute');
        if ($retcode != 0) {
            carp("benchbase exited with code $retcode");
        }


        chdir $project_path;

        say colored("copying ", "bold white"), 
            colored("Binary Log", "bold blue"),
            colored(" and changing ", "bold white"),
            colored("ownership", "bold blue");

        system(
            'sudo', 'sh', '-c',
            'cp db_data/server-binlog* db_data/*.ultproclog .'
        );

        system(
            'sudo', 'sh', '-c',
            sprintf('chown %s:%s ./server-binlog* ./*.ultproclog', $<, $>)
        );
    };

    chdir $cwd;

    say colored("stopping ", "bold white"), 
        colored("MariaDB", "bold blue"); 

    if (docker_compose($project_path, 'down') != 0) {
        croak("failed to stop MariaDB");
    }


    say colored("all done!", "bold green");

    say  
qq(
to run tests:
    cd runs/$project_path
    
    # set environment variables
    vi envvars
    
    # set key-columns, ... etc
    vi 01-create-cluster.sh
    vi 02-testcase-main.sh


    # run isolated MariaDB instance
    docker compose up -d

    # create statelog
    ./prepare-create-statelog.sh
    # create cluster
    ./01-create-cluster.sh
    # run test case
    ./02-testcase-main.sh

    # stop isolated MariaDB instance
    docker compose stop
);
}



sub main {
    if (scalar(@ARGV) != 2) {
        say usage;
        exit;
    }

    for my $varname (qw/ULTRAVERSE_HOME BENCHBASE_HOME/) {
        croak "environment variable $varname not set" unless $ENV{$varname};
    }

    my $benchmark  = $ARGV[0];
    my $txn_amount = $ARGV[1];

    unless ($txn_amount =~ m/^\d+[kmb]$/) {
        croak "invalid txn amount: $txn_amount";
    }

    bootstrap(
        $benchmark,
        $txn_amount,
        {
            path => {
                ultraverse => $ENV{'ULTRAVERSE_HOME'},
                benchbase => $ENV{'BENCHBASE_HOME'}
            }
        }
    );
}


main();
