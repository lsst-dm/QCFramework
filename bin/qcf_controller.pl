#!/usr/bin/perl -w

use strict;
use warnings;
use FileHandle;
#use QCFramework;
use QCF::QCFramework;
use Getopt::Long;
use Data::Dumper;
my ($fileList,$stdinBuffer,$desjob_dbid,$line,$infoHashref,$wrapperInstanceId,$node,$verbose,$filePath,$patternExecNames);

$verbose = 0;
Getopt::Long::GetOptions(
    "filelist=s"    => \$fileList,
    "execNames=s"    => \$patternExecNames,
    "wrapperInstanceId=i"     => \$wrapperInstanceId,
    "node=i"     => \$node,
    "verbose=i"     => \$verbose,
) or usage("Invalid command line options\n");

usage("Please supply the wrapperInstanceId parameter") unless defined $wrapperInstanceId;

        my $patternHash;
	print "\n ************** CALLING QCF in Controller with wrapperInstanceId $wrapperInstanceId ***************************";
	
	$infoHashref->{'desjob_dbid'} = $desjob_dbid;
	$infoHashref->{'exec_names'} = $patternExecNames;
	$infoHashref->{'wrapper_instance_id'} = $wrapperInstanceId;
	$infoHashref->{'node'} = $node;
	$infoHashref->{'verbose'} = $verbose;
	$infoHashref->{'filepath'} = $fileList;


	#my $qcFramework = QCF::QCFramework->new($infoHashref);
	my $qcFramework = QCFramework->new($infoHashref);
	### 
	# Open the file containing a list of all the files to be read.
	###
	if($fileList)
	{
		open (FH, "$fileList") or die "Cannot open $fileList $!";
		my @lines=<FH>;
		print "\n ### File List READ MODE###" if($verbose >=1);
		foreach $line (@lines) {
			chomp($line);
			#print "\n\n ----------- $line ---------";
			$qcFramework->extractQCData($line,$stdinBuffer,$infoHashref);
		}
	}
	else
	{
		print "\n ### STDIN READ MODE###" if($verbose >= 1);
		while (defined ($stdinBuffer = <STDIN>)){
			chomp($stdinBuffer);
			if(length($stdinBuffer) > 0){
			#print "\n\n\n #### ANKIT CHANDRA---->",$stdinBuffer,"<---------- ANKIT CHANDRA";
			$qcFramework->extractQCData($line,$stdinBuffer,$infoHashref);
			}
		}
	}

	#$qcFramework->insertProcessedVals();
	my $statusHashRef = "";#$qcFramework->getStatusData($infoHashref);
        print "\n the statusHashRef for wrapperInstanceId $wrapperInstanceId: ", Dumper($statusHashRef);

	
	print "\n QCF Controller has finished processing output. Exiting... \n ";

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
	  . " -filelist <log files in a list (separated by newline)>  -wrapperInstanceId <Id Of The Exec File From Exec Table> -execnames <comma separated names of executables>\n"
	  . "       filelist contains the list of files along with the full path. Either provide the filelist, or cat a file content to this script\n"
          . "       wrapperInstanceId is the id from the pfw_wrapper table which was run for this log.\n"
    );

    die("\n")

}

