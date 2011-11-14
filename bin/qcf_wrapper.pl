#!/usr/bin/perl -w


use strict;
use warnings;
use FileHandle;
use QCFramework;
use Getopt::Long;
use Data::Dumper;
use DB::FileUtils;

my ($fileList,$stdinBuffer,$desjob_dbid,$line,$infoHashref,$execDefsId,$node,$verbose,$filePath);

$verbose = 0;
Getopt::Long::GetOptions(
    "filelist=s"    => \$fileList,
    "verbose=i"     => \$verbose,
) or usage("Invalid command line options\n");

#usage("Please supply the execDefsId parameter") unless defined $execDefsId;
usage("Please supply the filelist ") unless defined $fileList;

##### qcf insertrows.pl script
my $archiveSiteStr;
my $getKeywords;
my $skipOnFileId;
my $filesHashref;
my $tempPath;
my @files;
##### end insertrows.pl script


        my $patternHash;
	
	$infoHashref->{'desjob_dbid'} = $desjob_dbid;
	$infoHashref->{'execdefs_id'} = $execDefsId;
	$infoHashref->{'node'} = $node;
	$infoHashref->{'verbose'} = $verbose;
	$infoHashref->{'filepath'} = $fileList;

#	my $qaFramework = QCFramework->new($infoHashref);
	### 
	# Open the file containing a list of all the files to be read.
	###
	if($fileList)
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
	}
	else
	{
		print "\n no filelist provided. exiting...";
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
	  . " -filelist <log files in a list (separated by newline)> \n"
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
	
	print "\n opening file $filepath";
	open( my $fileHandle, "<$filepath" );
	my @lines = <$fileHandle>;
	my @matched;
	my $rethash;
	my $exec;
	foreach my $line (@lines) {
		chomp($line);
		#print "\n\n ----------- $line ---------";
		###
		# Get the name of the module from the text in the file. the pattern is beginning <name of the module>
		###
		@matched = ($line =~ /\-\-(\s)*(beginning)\s*(\w*)(\s*)$/);
		if(scalar @matched > 0)
		{
			#print "\n matched! $3";
			$module = $3;	
		}
		
		###
		# Get the name of the executable from the string pattern: Executing <executable name>.pl	
		###
		if($line =~ /Executing\s*(\/[\w|\/]*){1,}(\.pl)?(\s)?.*/){
		$exec = $1.(defined $2 ? $2:'');	
		#print "\n got the final $exec";
		}
	}

	$rethash->{'module'} = $module;
	$rethash->{'run'} = $run;
	$rethash->{'module'} = $module;
	#print "\n the module is $module ";
	#@matched = ($filename =~ /(.*)_($module)_(.*)/);
	
	###
	# Get the name of the executable from the string pattern: Executing <executable name>.pl	
	###
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
	#my $dsn = "DBI:Oracle:host=141.142.226.50;sid=desoper_4";
	#my $pw = 'ank70chips';
	#my $user = 'ankitc';
	my $dsn = "DBI:Oracle:host=desdb.cosmology.illinois.edu;sid=des";
	my $user = 'des_admin';
	my $pw = 'deSadM1005';
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

	my $desjob_id = getnextId('desjob',$desdbh);

	### insert the row into the temp table
	#my $sqlInsertTempDesjob = "insert into desjob (id,name,block_id) values ($desjob_id, $desjob,$block_id)";
	my $sqlInsertTempDesjob = " MERGE INTO desjob USING (SELECT $desjob_id ID, \'$desjob\' NAME ,$block_id block_id  FROM dual) S ON (desjob.name = S.name and desjob.block_id = S.block_id) WHEN NOT MATCHED THEN INSERT (ID, name, block_id) VALUES (S.ID, S.name, S.block_id)";
	#print "\n the values to insert into desjob sql $sqlInsertTempDesjob : $desjob, $block_id";
	$sth = $desdbh->prepare($sqlInsertTempDesjob);
	$sth->execute() or print "\n err creating temp desjob table";
	$sth->finish();

	my $sqlfinalDesjobId = "select id from desjob where name = \'$desjob\' and block_id = $block_id ";
	#print "\n sqlfinaldesjobid $sqlfinalDesjobId";
	my $finalDesjob_id;
	$sth = $desdbh->prepare($sqlfinalDesjobId);
	$sth->execute() or print "\n err querying the latest exec id";
	while(my $row_fetchDesjobId = $sth->fetchrow_hashref()){
	
		$finalDesjob_id = $row_fetchDesjobId->{'id'};
	}


	my $module_id = getnextId('module',$desdbh);
	my $module_name;
	### create the temp table for module to use in merge
        #my $sqlInsertTempModule = "insert into module (id,desjob_id,block_id) values ($module_id, $desjob_id,$block_id)";
	my $sqlInsertTempModule = " MERGE INTO module USING (SELECT $module_id ID, $finalDesjob_id desjob_id ,$block_id block_id, \'$module\' name FROM dual) S ON (module.name = S.name and module.desjob_id = S.desjob_id) WHEN NOT MATCHED THEN INSERT (ID, desjob_id, block_id,name) VALUES (S.ID, S.desjob_id, S.block_id,S.name)";
        #print "\n the values to insert into module sql $sqlInsertTempModule : $desjob_id, $block_id";
        $sth = $desdbh->prepare($sqlInsertTempModule);
        $sth->execute() or print "\n err creating temp desjob table";
	$sth->finish();


	my $sqlfinalModuleId = "select id from module where name = \'$module\' and desjob_id = $finalDesjob_id ";
	my $finalModule_id;
	$sth = $desdbh->prepare($sqlfinalModuleId);
	$sth->execute() or print "\n err querying the latest exec id";
	while(my $row_fetchModuleId = $sth->fetchrow_hashref()){
		$finalModule_id = $row_fetchModuleId->{'id'};
	}


	my $exec_id = getnextId('exec',$desdbh);
	my $sqlMergeExec = " MERGE INTO exec USING (SELECT $exec_id ID, \'$exec\' name, \'$exec\' path FROM dual) S ON (exec.path = S.path) WHEN NOT MATCHED THEN INSERT (ID, name, path) VALUES (S.ID, S.name, S.path)";
	#print "\n the exec merge query, $sqlMergeExec";
	$sth = $desdbh->prepare($sqlMergeExec);
	$sth->execute() or print "\n err querying the merge query";
	$sth->finish();

	my $sqlfinalExecId = "select id from exec where path like \'%$exec%\'";
	$sth = $desdbh->prepare($sqlfinalExecId);
	$sth->execute() or print "\n err querying the latest exec id";
	while(my $row_fetchExecId = $sth->fetchrow_hashref()){
	
		$exec_id = $row_fetchExecId->{'id'}
	}
	$sth->finish();
	#print "\n the values to insert into  exec sql $sqlInsert: $exec, $exec_id";	

	my $execdefs_id = getnextId('execdefs',$desdbh);
	my $sqlInsert = " MERGE INTO execdefs USING (SELECT $execdefs_id id,$exec_id exec_id , $finalModule_id module_id, $finalDesjob_id desjob_dbid FROM dual) S ON (execdefs.exec_id = S.exec_id and execdefs.module_id = S.module_id and execdefs.desjob_dbid = S.desjob_dbid) WHEN NOT MATCHED THEN INSERT (ID, exec_id, module_id,  desjob_dbid) VALUES (S.ID, S.exec_id, S.module_id, S.desjob_dbid)";
	#my $sqlInsert = "insert into execdefs (id,exec_id,module_id, desjob_dbid) values ($execdefs_id,$exec_id, $module_id, $block_id)";
	#print "\n the values to insert into  execdefs sql $sqlInsert: $exec_id, $module_id, $block_id, $execdefs_id";	
	$sth = $desdbh->prepare($sqlInsert);
	$sth->execute() or print "\n err inserting into execdefs";
	$sth->finish();

	my $sqlfinalExecDefsId = "select id from execdefs where desjob_dbid = $finalDesjob_id and exec_id = $exec_id and module_id = $finalModule_id ";
	$sth = $desdbh->prepare($sqlfinalExecDefsId);
	$sth->execute() or print "\n err querying the latest exec id";
	my $finalExecDefs_id;	
	while(my $row_fetchExecDefsId = $sth->fetchrow_hashref()){
	
		$finalExecDefs_id = $row_fetchExecDefsId->{'id'};
	}

	$desdbh->commit();
	$desdbh->disconnect();

#	print "\n the final execDefsId  for $sqlfinalExecDefsId is $finalExecDefs_id";
	readpipe "cat $filepath | perl qcf_controller.pl -execDefsId $finalExecDefs_id -verbose 1 > controllerout";
	close($fileHandle);
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
