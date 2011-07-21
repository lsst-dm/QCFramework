#!/usr/bin/perl -w


use strict;
use warnings;
use FileHandle;
use Getopt::Long;
use Data::Dumper;
use DB::DESUtil;

        my $patternHash;
	my ($fileList,$stdinBuffer,$line,$infoHashref,$node);
	my ($execdefs_id,$project,$run,$desjob_id);
	
Getopt::Long::GetOptions(
    "execdefs_id=i"    => \$execdefs_id, # this is the unique dbid which can be used to get information about a unique job. if this is not present, we will need other parameters like run, project and desjob_id to come close to identifying a job.
    "project:s"     => \$project,
    "run:s"     => \$run,
    "desjob_id:s"     => \$desjob_id,
) or usage("Invalid command line options\n");

usage("You must supply atleast execdefs_id to proceed") unless defined $execdefs_id;

#
# Make a database connection
#
my $desdbh = DB::DESUtil->new(

    DBIattr => {
        AutoCommit => 0,
        RaiseError => 1,
        PrintError => 0
    }
);




	
sub usage {

    my $message = $_[0];
    if ( defined $message && length $message ) {
        $message .= "\n"
          unless $message =~ /\n$/;
    }

    my $command = $0;
    $command =~ s#^.*/##;

    print STDERR (
        $message,
        "\n"
          . "usage: $command "
          . " -execdefs_id ExecDefsTableId -project ProjectNameString -run RunNumber -desjob_id DESJobSubmitId \n"
          . "       DESJobIdUniqueDBId is the unique DB id associated in the DB with every DB Job.\n"
          . "       if desjob_dbid is NOT known, Jobs can be identified by providing a bunch of other variables together:\n"
          . "          project: The name of the project 'DES', run: The run number, desjob_id: The submit ID for jobs\n"
    );

    die("\n")

}

