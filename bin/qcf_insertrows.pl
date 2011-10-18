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

my $patternHash;
my ($filePath,$verbose);
my ($tableHash,$whereHash,$statusArr,$whereColArr,$tableColArr,$desjobIdArr,$runArr,$colArr,$parentTag,$currTable,$whereTableName,$whereColVal,$allTablesNeeded,$sqlWhereColumns,$sqlFrom,$sqlFinal,$desjob_dbid,$run,$desjob_id );
my ($fileList,$infoHashref);
	
Getopt::Long::GetOptions(

    "fileList=s"     => \$fileList,
    "verbose:i"     => \$verbose,
) or usage("Invalid command line options\n");

usage("\nYou must provide the file path to proceed") unless defined $fileList;
	
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
#my $fileDets = extractDets(@$resolvedFilenamesArrRef);
die "done";


sub extractDets {
	my ($resolvedFile) = @_;
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
	
	open( my $fileHandle, "<$logfilepath" );
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
		#print "\n MATHCED! the text from this is block $1  moduke $2 desjob $3 module otherwse $module and filename $filename",Dumper($rethash);
	}
	$rethash->{'desjob'} = $desjob;
	$rethash->{'block'} = $block;

	# module: id, desjobid, blockid
	# execdefs: id, moduleid, desjobid, execid	
	# desjob: id, blockid
	my $dsn = "DBI:Oracle:host=desdb.cosmology.illinois.edu;sid=des";
	my $pw = 'deSadM1005';
	my $user = 'des_admin';
	my $row;
	my $desdb_dbh = DBI->connect($dsn, $user, $pw);	
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

	my $sqlInsert = "insert into desjob (id, block_id) values ($desjob,$block_id)";
	#print "\n the values to insert into desjob sql $sqlInsert : $desjob, $block_id";	
	$sth = $desdbh->prepare($sqlInsert);
	$sth->execute() or print "\n err inserting into desjob";
	$sth->finish();

	my $module_id = getnextId('module',$desdbh);
	$sqlInsert = "insert into module (id, desjob_id, block_id) values ($module_id, $desjob,$block_id)";
	#print "\n the values to insert into  module sql $sqlInsert : $desjob,$module_id, $block_id";	
	$sth = $desdbh->prepare($sqlInsert);
	$sth->execute() or print "\n err inserting into module";
	$sth->finish();

	my $exec_id = getnextId('exec',$desdbh);
	$sqlInsert = "insert into exec (id, path, name) values ($exec_id, \'$exec\', \'$exec\')";
	#print "\n the values to insert into  exec sql $sqlInsert: $exec, $exec_id";	
	$sth = $desdbh->prepare($sqlInsert);
	$sth->execute() or print "\n err inserting into exec";
	$sth->finish();

	my $execdefs_id = getnextId('execdefs',$desdbh);
	$sqlInsert = "insert into execdefs (id,exec_id,module_id, desjob_dbid) values ($execdefs_id,$exec_id, $module_id, $block_id)";
	#print "\n the values to insert into  execdefs sql $sqlInsert: $exec_id, $module_id, $block_id, $execdefs_id";	
	$sth = $desdbh->prepare($sqlInsert);
	$sth->execute() or print "\n err inserting into execdefs";
	$sth->finish();

	$desdbh->commit();
	$desdbh->disconnect();

	print "\n the final execDefsId is $execdefs_id";
	print "\n ####### DONE #######";
}

sub getnextId {

	my ($table,$desdbh) = @_;
	#
	# Query the oracle sequencer for the location table
	#

  my $outputId = 0;
  my $sql = " SELECT ".$table."_id.nextval FROM dual";

  my $sth=$desdbh->prepare($sql);
  $sth->execute();
  $sth->bind_columns(\$outputId);
  $sth->fetch();
  $sth->finish();

	#print "\n sending $table id as $outputId";
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


