#!/usr/bin/perl -w


use strict;
use warnings;
use FileHandle;
use QCFramework;
use Getopt::Long;

my ($fileList,$stdinBuffer,$desjob_dbid,$line,$infoHashref,$execTableId,$node,$verbose);

$verbose = 0;
Getopt::Long::GetOptions(
    "filelist=s"    => \$fileList,
    "execTableId=i"     => \$execTableId,
    "node=i"     => \$node,
    "verbose=i"     => \$verbose,
) or usage("Invalid command line options\n");

usage("Please supply the execTableId parameter") unless defined $execTableId;

        my $patternHash;
	print "\n ************** CALLING QAF IN Controller with execTableId $execTableId";
	
	$infoHashref->{'desjob_dbid'} = $desjob_dbid;
	$infoHashref->{'execdefs_id'} = $execTableId;
	$infoHashref->{'node'} = $node;
	$infoHashref->{'verbose'} = $verbose;

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
			$qaFramework->extractQAData($line,$stdinBuffer,$infoHashref);
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
	  . " -filelist files -desjob_dbid UniqueDBIdForJob -execTableId IdOfTheExecFileFromExecTable\n"
	  . "       filelist contains the list of files along with the full path\n"
          . "       desjob_dbid is the unique Database ID for the DESJob\n"
          . "       execTableId is the id of the executable file which was run for this log.\n"
    );

    die("\n")

}

