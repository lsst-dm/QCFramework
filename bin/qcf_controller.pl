#!/usr/bin/perl -w


use strict;
use warnings;
use FileHandle;
use QCFramework;
use Getopt::Long;

my ($fileList,$stdinBuffer,$desjob_dbid,$line,$infoHashref,$execDefsId,$node,$verbose,$filePath);

$verbose = 0;
Getopt::Long::GetOptions(
    "filelist=s"    => \$fileList,
    "execDefsId=i"     => \$execDefsId,
    "node=i"     => \$node,
    "verbose=i"     => \$verbose,
) or usage("Invalid command line options\n");

usage("Please supply the execDefsId parameter") unless defined $execDefsId;

        my $patternHash;
	print "\n ************** CALLING QCF in Controller with execDefsId $execDefsId";
	
	$infoHashref->{'desjob_dbid'} = $desjob_dbid;
	$infoHashref->{'execdefs_id'} = $execDefsId;
	$infoHashref->{'node'} = $node;
	$infoHashref->{'verbose'} = $verbose;
	$infoHashref->{'filepath'} = $fileList;

	my $qaFramework = QCFramework->new($infoHashref);
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
			$qaFramework->extractQAData($line,$stdinBuffer,$infoHashref);
		}
	}
	else
	{
		print "\n ### STDIN READ MODE###" if($verbose >= 1);
		while (defined ($stdinBuffer = <STDIN>)){
			chomp($stdinBuffer);
			if(length($stdinBuffer) > 0){
			#print "\n\n\n #### ANKIT CHANDRA---->",$stdinBuffer,"<---------- ANKIT CHANDRA";
			$qaFramework->extractQAData($line,$stdinBuffer,$infoHashref);
			}
		}
	}

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
	  . " -filelist <log files in a list (separated by newline)>  -execDefsId <Id Of The Exec File From Exec Table>\n"
	  . "       filelist contains the list of files along with the full path. Either provide the filelist, or cat a file content to this script\n"
          . "       desjob_dbid is the unique Database ID for the DESJob\n"
          . "       execDefsId is the id from the execdefs table which was run for this log.\n"
    );

    die("\n")

}

