#!/usr/bin/perl -w

use strict;
use warnings;
use FileHandle;
use QCFramework;
use Getopt::Long;
use Data::Dumper;

use Benchmark;
use Cwd;
use Data::Dumper;
use FindBin qw($Bin);
use File::Basename;
use File::stat;
use Getopt::Long;
use Time::localtime;
use DBI;
use XML::Simple;
use DBD::CSV;
use Getopt::Long;
use Switch;
use DB::DESUtil;
use DB::FileUtils;
use FileHandle;


die ("##### DEPRECATED. USE QCF_WRAPPER INSTEAD #####")
my $patternHash;

## Command line input params
my ($outputfile,$filePath,$verbose);
my ($tableHash,$whereHash,$statusArr,$whereColArr,$tableColArr,$desjobIdArr,$runArr,$colArr,$parentTag,$currTable,$whereTableName,$whereColVal,$allTablesNeeded,$sqlWhereColumns,$sqlFrom,$sqlFinal,$desjob_dbid,$run,$desjob_id );
my ($fileList,$infoHashref);
	
Getopt::Long::GetOptions(

    "fileList=s"     => \$fileList,
    "outputfile=s"     => \$outputfile,
    "verbose:i"     => \$verbose,
) or usage("Invalid command line options\n");

usage("\nYou must provide the file path to proceed") unless defined $fileList;
usage("\nYou must provide the outputfile path to proceed") unless defined $outputfile;
	
	$infoHashref->{'desjob_dbid'} = $desjob_dbid;
	$infoHashref->{'run'} = $run;
	$infoHashref->{'desjob_id'} = $desjob_id;
	$infoHashref->{'verbose'} = $verbose;


#
# Read in the filelist
#
my @files = ();
readFileList( $fileList, \@files );




my $archiveSiteStr;
my $getKeywords;
my $skipOnFileId;

my ( $resolvedFilenamesArrRef, $runIDS, $nites, $project ) =
  parseFilelist( \@files,  $archiveSiteStr,  $getKeywords, $skipOnFileId );


print "\n the files", Dumper($resolvedFilenamesArrRef);
foreach my $resolvedFilenamesSingle (@{$resolvedFilenamesArrRef}){
#print "\n the files", Dumper($resolvedFilenamesSingle);

 extractDets($outputfile,@$resolvedFilenamesSingle);
}
die "done";


sub extractDets {
	my ($outputfile,$resolvedFile) = @_;
	my ($project,$run,$block,$module,$desjob,$filename);
	#print "\n the file ",Dumper(@$resolvedFile);
	my @matchedArr;
	my $logfilepath;
	foreach my $tempHash ($resolvedFile){
		#$logfilepath = %$tempHash->{'LOCALPATH'}.'/'.%$tempHash->{'FILENAME'};#$resolvedFile->{'LOCALPATH'}.'/'.%$resolvedFile->{'FILENAME'};
		$logfilepath = $tempHash->{'LOCALPATH'}.'/'.$tempHash->{'FILENAME'};#$resolvedFile->{'LOCALPATH'}.'/'.%$resolvedFile->{'FILENAME'};
		$project = $tempHash->{'PROJECT'};
		$run = $tempHash->{'RUN'};
		$filename =  $tempHash->{'FILENAME'};
	}
	
	open( my $fileHandle, "<$logfilepath" );
	open( FH_write, ">$outputfile" );
	my @lines = <$fileHandle>;
	my @matched;
	my $rethash;
	my $exec;
	foreach my $line (@lines) {
		chomp($line);
		#print "\n\n ----------- $line ---------";
		@matched = ($line =~ /\-\-(\s)*(beginning)\s*(\w*)(\s*)$/);
		if(scalar @matched > 0)
		{
			#print "\n matched! $3";
			$module = $3;	
		}
		
		if($line =~ /Executing\s*(\/[\w|\/]*){1,}(\.pl)?(\s)?.*/){
		#print "\n got the exec $1 $2 ";
		$exec = $1.$2;	
		}
	}

	$rethash->{'module'} = $module;
	$rethash->{'run'} = $run;
	$rethash->{'module'} = $module;
	#@matched = ($filename =~ /(.*)_($module)_(.*)/);
	my $regexCompiled =  qr/(.*)_($module)_(.*)(\.)(\w*)$/;
	@matched = ($filename =~ $regexCompiled);
	if(scalar @matched > 0) {
		$desjob = $3;
		$block = $1;
		print "\n MATCHED! the text from this is block $1  module $2 desjob $3 module otherwise $module and filename $filename",Dumper($rethash);
	}
	$rethash->{'desjob'} = $desjob;
	$rethash->{'block'} = $block;

	# module: id, desjobid, blockid
	# execdefs: id, moduleid, desjobid, execid
	# desjob: id, blockid
	my $DBIattr;
	my $dsn = "DBI:Oracle:host=leovip148.ncsa.uiuc.edu;service_name=desoper";
	my $user = 'ankitc';
	my $pass = 'ank70chips';
	my $connectDescriptor = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=leovip148.ncsa.uiuc.edu)(PORT=1521))(CONNECT_DATA=(SERVER=dedicated)(SERVICE_NAME=desoper)))";
	#my $desdb_dbh = $self->SUPER::connect("DBI:Oracle:$connectDescriptor",$user,$pass, $DBIattr) or croak("Database connection error: $DBI::errstr\n");
	my $row;
	my $desdb_dbh = DBI->connect($dsn, $user, $pass);	
	my $sth;
	my $sql = "select run.id, block.block_id, block.run_id from block, run where block.run_id = run.id and run.run = \'$run\' and block_name = \'$block\'";
	$sth = $desdb_dbh->prepare($sql);
	$sth->execute();
	$row = $sth->fetchrow_hashref();
	#print "\n SELECT  for $sql",Dumper($row);
	my $block_id = $row->{'BLOCK_ID'};
	$sth->finish();
	$desdb_dbh->disconnect();

#
# Make a database connection
#
my $desdbh = DB::DESUtil->new();

	my $job_id = getnextId('pfw_job',$desdbh);
	my $sqlInsert = "insert into pfw_job (id, name ,block_id) values ($job_id,  "."'".$desjob."'".",$block_id)";
	print "\n the values to insert into pfw_job sql are: $sqlInsert : $desjob, $block_id";	
	$sth = $desdbh->prepare($sqlInsert);
	$sth->execute() or print "\n err inserting into desjob";
	$sth->finish();

	my $module_id = getnextId('pfw_module',$desdbh);
	$sqlInsert = "insert into pfw_module (id, pfw_job_id, block_id) values ($module_id,  $job_id ,$block_id)";
	print "\n the values to insert into  pfw_module sql $sqlInsert : $desjob,$module_id, $block_id";	
	$sth = $desdbh->prepare($sqlInsert);
	$sth->execute() or print "\n err inserting into module";
	$sth->finish();

	my $exec_id = getnextId('pfw_executable',$desdbh);
	$sqlInsert = "merge into pfw_executable A using (select '".$exec."' path from dual ) B on ( A.path = B.path) WHEN NOT MATCHED THEN INSERT  (A.id, A.path, A.name) values ($exec_id, \'$exec\', \'$exec\')";
	#$sqlInsert = "insert into pfw_executable (id, path, name) values ($exec_id, \'$exec\', \'$exec\')";
	print "\n the values to insert into  pfw_executable sql $sqlInsert: $exec, $exec_id";
	$sth = $desdbh->prepare($sqlInsert);
	$sth->execute() or print "\n err inserting into exec";
	$sth->finish();

	my $sqlSelect = "select id from pfw_executable where path like '%".$exec."%'";
	$sth = $desdbh->prepare($sqlSelect);
	$sth->execute() or print "\n err inserting into exec";
	my $rowExecTableId = $sth->fetchrow_hashref();
	$exec_id = $rowExecTableId->{'id'};
	$sth->finish();

	my $execdefs_id = getnextId('pfw_executable_def',$desdbh);
	$sqlInsert = "insert into pfw_executable_def (id,pfw_executable_id,pfw_module_id, pfw_job_id) values ($execdefs_id,$exec_id, $module_id, $block_id)";
	print "\n the values to insert into  pfw_executable_def sql $sqlInsert: $exec_id, $module_id, $block_id, $execdefs_id";	
	$sth = $desdbh->prepare($sqlInsert);
	$sth->execute() or print "\n err inserting into execdefs";
	$sth->finish();

	$desdbh->commit();
	$desdbh->disconnect();


	print FH_write "\n$logfilepath : $execdefs_id";
	print "\n the final execDefsId is $execdefs_id";
	print "\n ####### DONE #######";
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

	print "\n sending $table id as $outputId";
  return $outputId;
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
           "\nThe purpose of this script is to insert missing rows into MODULES, DESJOB, EXEC, EXECDEFS tables  \n"
          . "\n\tusage: perl qcf_insertrows.pl -filelist <filelist> "
          . "\n\tgive it the filelist just as you do to fileingest.\n"
    );

    die("\n")

}


