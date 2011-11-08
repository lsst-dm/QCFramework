#!/usr/bin/perl -w


use strict;
use warnings;
use FileHandle;
use Getopt::Long;
use Data::Dumper;

        my $patternHash;
	my ($fileList,$stdinBuffer,$line,$infoHashref,$node,$filePath);	
	my ($desjob_dbid,$project,$run,$desjob_id,$verbose,$module_id,$block_id);
	
Getopt::Long::GetOptions(
    "desjob_dbid=i"    => \$desjob_dbid, # this is the unique dbid which can be used to get information about a unique job. if this is not present, we will need other parameters like run, project and desjob_id to come close to identifying a job.
    "moduleId:s"     => \$module_id,
    "blockId:s"     => \$block_id,
    "run:s"     => \$run,
    "desjobId:s"     => \$desjob_id,
    "verbose:i"     => \$verbose,
    "filePath:s"     => \$filePath,
) or usage("Invalid command line options\n");

usage("You must supply atleast one of blockId, moduleId, run, desjobId to proceed") if  (not defined $desjob_id && not defined $run && not defined $block_id && not  defined $module_id);
	
use QCFramework;

	$infoHashref->{'project'} = $project;
	$infoHashref->{'run'} = $run;
	$infoHashref->{'desjob_dbid'} = $desjob_id;
	$infoHashref->{'verbose'} = $verbose;
	$infoHashref->{'filepath'} = $filePath;

	my $qaFramework = QCFramework->new($infoHashref);

	#print "\n ### $desjob_dbid";
	print Dumper($infoHashref);
	#my $statusHashRef = $qaFramework->getQAStatus($infoHashref);
	my $statusHashRef = $qaFramework->getStatusData($infoHashref);
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
          . " -desjobId DESJobID -moduleId <module's Id> -run RunNumber -blockId <ID of the block> \n"
          #. "       (to be implemented)if desjob_dbid is NOT known, Jobs can be identified by providing a bunch of other variables together:\n"
    );

    die("\n")

}

