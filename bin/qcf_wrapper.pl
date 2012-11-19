#!/usr/bin/perl -w


use strict;
use warnings;
use FileHandle;
use QCFramework;
#use QCF::QCFramework; nn
use Getopt::Long;
use Data::Dumper;
use DB::FileUtils;
use File::Find;

my ($fileList,$stdinBuffer,$desjob_dbid,$line,$infoHashref,$wrapperInstanceId,$node,$verbose,$filePath,$noingest,$dir,$wrapperInstanceId,$execNames) = ();

$verbose = 0;
Getopt::Long::GetOptions(
    "filelist=s"    => \$fileList,
    "execNames=s"    => \$execNames,
    "dir=s"    => \$dir,
    "noingest"    => \$noingest,
    "verbose=i"     => \$verbose,
    "wrapperInstanceId=i"     => \$wrapperInstanceId,
) or usage("Invalid command line options\n");

usage("Please supply the wrapperInstanceId") unless defined $wrapperInstanceId;
usage("Please supply the execNames") unless defined $execNames;

if(defined (<STDIN>)){
	$mode = 'stdin';
}else{
	$mode = 'files';
}

#usage("Please supply the filelist ") unless (defined $fileList || defined $dir);




##### qcf insertrows.pl script
my $archiveSiteStr;
my $getKeywords;
my $skipOnFileId;
my $filesHashref;
my $tempPath;
my @files;
##### end insertrows.pl script


        my $patternHash;
	
	$infoHashref->{'wrapperinstance_id'} = $wrapperInstanceId;
	$infoHashref->{'verbose'} = $verbose;
	$infoHashref->{'filepath'} = $fileList;

#	my $qcFramework = QCFramework->new($infoHashref);
	### 
	# Open the file containing a list of all the files to be read.
	###
	if($mode eq 'files')
	{
		open (FH, "$fileList") or die "Cannot open $fileList $!";
		my @lines=<FH>;
		foreach $line (@lines) {
			chomp($line);
			@files = ();
			$tempPath = $line;
			$tempPath =~ s/^(.*?)\/Archive\///;	
			$tempPath =~ /^(.*)\/(\S*\.\S*)$/;	
				
			$filesHashref->{'localfilename'} = $2;
			$filesHashref->{'localpath'} = $1;
			$filesHashref->{'fileid'} = 0;
			push @files, $filesHashref;
		#	print "\n the files array", Dumper(@files);
			print "\n\n ----------- $line ---------";
			my ( $resolvedFilenamesArrRef, $runIDS, $nites, $project ) =
			  parseFilelist( \@files,  $archiveSiteStr,  $getKeywords, $skipOnFileId );
		#	print "\n the files", Dumper($resolvedFilenamesArrRef);
			my $fileDets = extractDets(@$resolvedFilenamesArrRef,$line);
		}
	}elsif($mode eq 'stdin'){
			while(defined ($stdinBuffer = <STDIN>)){
			readpipe "cat $stdinBuffer | perl qcf_controller.pl -execNames $execNames -wrapperInstanceId $wrapperInstanceId -verbose $verbose >> controllerout";
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
	  . " -filelist <log files in a list (separated by newline)> [-noingest] -dir <IN DEVELOPMENT point to a directory to bypass creating a file>\n"
	  . "       filelist contains the list of files along with the full path. Either provide the filelist, or cat a file content to this script\n"
    );

    die("\n")

}


sub extractDets {
	my ($resolvedFile,$filepath) = @_;
	my ($project,$run,$block,$module,$desjob,$filename);
	#print "\n the file ",Dumper(@$resolvedFile);
	my @matchedArr;
	my $logfilepath;
	foreach my $tempHash (@$resolvedFile){
		#$logfilepath = %$tempHash->{'LOCALPATH'}.'/'.%$tempHash->{'FILENAME'};#$resolvedFile->{'LOCALPATH'}.'/'.%$resolvedFile->{'FILENAME'};
		$logfilepath = $tempHash->{'LOCALPATH'}.'/'.$tempHash->{'FILENAME'};#$resolvedFile->{'LOCALPATH'}.'/'.%$resolvedFile->{'FILENAME'};
		$project = $tempHash->{'PROJECT'};
		$run = $tempHash->{'RUN'};
		$filename =  $tempHash->{'FILENAME'};
	}
	
	if (!$noingest){
		readpipe "cat $filepath | perl qcf_controller.pl -wrapperInstanceId $finalExecDefs_id -verbose $verbose >> controllerout";
	}
	else{
		print "\n #### NOT INGESTING ####";
	}
	close($fileHandle);
	print "\n ####### DONE ####### \n ";
}



sub getnextId {

	my ($table,$desdbh) = @_;
	#
	# Query the oracle sequencer for the location table
	#

  my $outputId = 0;
  my $sql = " SELECT ".$table."_seq.nextval FROM dual";

  my $sth=$desdbh->prepare($sql);
  $sth->execute();
  $sth->bind_columns(\$outputId);
  $sth->fetch();
  $sth->finish();

	#print "\n sending $table id as $outputId";
  return $outputId;
}
