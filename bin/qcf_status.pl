#!/usr/bin/perl -w


use strict;
use warnings;
use FileHandle;
use QCFramework;
use Getopt::Long;
use Data::Dumper;

        my $patternHash;
	my ($fileList,$stdinBuffer,$line,$infoHashref,$node,$filePath);	
	my ($desjob_dbid,$project,$run,$desjob_id,$verbose);
	
Getopt::Long::GetOptions(
    "desjob_dbid=i"    => \$desjob_dbid, # this is the unique dbid which can be used to get information about a unique job. if this is not present, we will need other parameters like run, project and desjob_id to come close to identifying a job.
    "project:s"     => \$project,
    "run:s"     => \$run,
    "desjob_id:s"     => \$desjob_id,
    "verbose:i"     => \$verbose,
    "filePath:s"     => \$filePath,
) or usage("Invalid command line options\n");

usage("You must supply atleast desjob_dbid to proceed") unless defined $desjob_dbid;
	
	$infoHashref->{'desjob_dbid'} = $desjob_dbid;
	$infoHashref->{'project'} = $project;
	$infoHashref->{'run'} = $run;
	$infoHashref->{'desjob_id'} = $desjob_id;
	$infoHashref->{'verbose'} = $verbose;
	$infoHashref->{'filepath'} = $filePath;

	my $qaFramework = QCFramework->new($infoHashref);

	#print "\n ### $desjob_dbid";
	my $statusHashRef = $qaFramework->getQAStatus($infoHashref);
	print "\n the statusHashRef: ", Dumper($statusHashRef);

	
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
          . " -desjob_dbid DESJobID -project ProjectNameString -run RunNumber -desjob_id DESJobSubmitId \n"
          . "       DESJobId is the unique DB id associated in the DB with every DB Job.\n"
          #. "       (to be implemented)if desjob_dbid is NOT known, Jobs can be identified by providing a bunch of other variables together:\n"
          . "          project: The name of the project 'DES', run: The run number, desjob_id: The submit ID for jobs\n"
    );

    die("\n")

}

