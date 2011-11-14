
package QCFramework;
use 5.008008;
use strict;
use warnings;
use Data::Dumper;
use FileHandle;
use DB::DESUtil;
use RegexContainer;
use POSIX 'strftime';
use DBI;
use Data::Dumper;
use Switch;

if (defined($ENV{'DES_HOME'})) {
   use lib $ENV{'DES_HOME'}."/lib/perl5";
}



require Exporter;

our @ISA = qw(Exporter);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration	use QCFramework ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.
our %EXPORT_TAGS = ( 'all' => [ qw(test
	
) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(test
	
);

our $VERSION = '0.01';

my $verbose;
$verbose = 0;
# Preloaded methods go here.

sub new {
        my $self  = {};
	my ($class,$infoHashref) = @_;
	my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
	$self->{_desdbh} = DB::DESUtil->new(DBIattr => {AutoCommit => 0, RaiseError => 1, PrintError => 0   });
	$self->{_timestamp} = "to_date('$yday-$year $hour-$min-$sec', 'DDD-yyy HH24-MI-SS')";

	if(defined $infoHashref->{'execdefs_id'}){
		$self->{_regexContainer} = RegexContainer->new($infoHashref) ;
	}
	$verbose = $infoHashref->{'verbose'} if defined $infoHashref->{'verbose'};
        bless($self);           # but see below
	#$sql = "select * from qa_patterns";
        return $self;
    }


sub extractQAData
{
	my ($self,$filePath,$buffer,$infoHashref) = @_;
	my ($fileHandle, $mode);
	my $regexPattern;
	my @matchedArr;
	my $regexCompiled;
	my $insertHashRef;
	my @insertVals;
	my $regexHashTemp;
	my @insertVarNames;
	my @varHash;
	my $varHashTemp;
	my @runids;
	my @timestamps;
	my @nodes;
	my $now_time;
	# Thu Nov 10 11:04:30 2011
	#my $sqlInsert = "insert into qa_output (QAVARIABLES_ID,VALUE,TS,NODE,EXECDEFS_ID,ID,IMAGE) values (?,?,to_date(?,'DD-MM-YYYY HH24:MI:SS'),?,?,?,?)";
	my $sqlInsert = "insert into qa_output (QAVARIABLES_ID,VALUE,TS,NODE,EXECDEFS_ID,ID,IMAGE) values (?,?,to_date(?,'DY MON DD HH24:MI:SS YYYY'),?,?,?,?)";

        my $insertSth = $self->{_desdbh}->prepare($sqlInsert) or print "Error in preparing $!";
	my $i;
	my @lines;
 	my $outputId;	
	my (@variableIdArr, @extractedValue, @extraInfoArr, @execTableIdArr, @outputIdArr,@tuple_status);
	my $imageId;
	###if(defined $verbose){print "\n\n \t PATTERNS OBJECT FROM DB: \n\n \t"; print Dumper($regexContainerObject);}
	if( $verbose >= 2){print "\n\n \t In SubRoutine extractQAData: PATTERNS OBJECT FROM DB: \n\n \t"; print Dumper($self->{_regexContainer});}

	### 
	# If there is a filepath given to the function, then use that to open the file and read the data. 
	###	
	if($filePath) {

		$mode = "file";
		if( $verbose >= 1){print "\n\n ##### Opening File $filePath  #####\n\n";}
		open (FH, "$filePath") or die "Cannot open $filePath $!";
	 	@lines=<FH>;
	}
	elsif($buffer)
	{
		$mode = "buffer";
		@lines = split("\n",$buffer);
	}
	if($verbose >= 1){print "\n\n ##### Processing lines from $mode...  #####\n\n";}

	foreach my $line (@lines) {
		chomp($line);
		if( $verbose >= 2){print "\n\n Working on line $line\n\n";}
		if( $verbose >= 2){print "\n\n the regexHash Object\n\n",Dumper($self->{_regexContainer}->{regex_hash});}
		###foreach my $regexObj ($regexContainerObject->{regex_hash}) {
		foreach my $regexObj ($self->{_regexContainer}->{regex_hash}) {
			foreach my $regexHash (@$regexObj) { # Loop through the patterns
				$regexPattern = $regexHash->{'pattern'};
				if( $verbose >= 1){print "\n\n Current Pattern: $regexPattern\n\n";}
				$regexCompiled = qr/$regexPattern/sm;
				##### match the regular expression #####
				#### matched array matchedArr has the values extracted from the line above ####
				@matchedArr = ($line =~ $regexCompiled);
				# 3rd argument is what is 
				if(scalar @matchedArr > 0 && execMatched($self,$regexHash,$self->{_regexContainer}->{'exec_id'})) {
			
				undef @variableIdArr;
				undef @extractedValue;
				undef @extraInfoArr;
				undef @execTableIdArr;
				undef @outputIdArr;
				### Store the line that matched, in to the database ###
				#storeQAFMessage($self, $line,$infoHashref->{'execdefs_id'},$regexHash->{'pattern_id'});
				storeQAFMessage($self, $line,$infoHashref->{'execdefs_id'},$regexHash->{'id'});
				### do not continue with the variables if the exec_id is 0. since thats for status messages only
				next if($regexHash->{'exec_id'} == 0); 
				if( $verbose >= 1){ print "\n\n \t Line $line matched with variables \n\n";}
				if($verbose >=2){ print " : ",Dumper(@matchedArr);}

	#### each varHashTemp has values for each variable contained in a pattern. Extract those values and store it into the DB ####
					foreach $varHashTemp ($regexHash->{'variables'}){
						for ($i=0;$i<scalar @$varHashTemp;$i++){
							
							if(defined @$varHashTemp[$i]->{'action_code'}){	
							switch(@$varHashTemp[$i]->{'action_code'}){
	#### Case 2 of action_code means that the value from this variable will be used as the Image source for all variables to come from other patterns, untill it is overridden by some other Image variable from a different pattern
								case '2' {
									$imageId = stripFilePath($self, $matchedArr[$i] );
									print "\n Case 2: Setting Image id to $imageId" if ($verbose >= 1);
								}
								case '1' {

	#### If the id of the variable matches its image id, then this is variable is the image id for the entire set of variables caught. Store the id in to the table (currently we store the entire path of the image variable (minus the node specific part, the first 2 directories)       
									if (@$varHashTemp[$i]->{'id'} == @$varHashTemp[$i]->{'image_id'}){
									#$imageId = $outputId;
									$imageId = stripFilePath($self, $matchedArr[$i] );;
									}
								}
							}
							}
							$outputId = getNextOutputID($self);
							# create the Variable name column for insertion	
							push @variableIdArr, @$varHashTemp[$i]->{'id'};
							push @extractedValue, $matchedArr[$i];
							push @extraInfoArr, $infoHashref->{'node'};
							push @execTableIdArr, $infoHashref->{'execdefs_id'};
							push @outputIdArr, $outputId;
							
						}

						$insertSth->bind_param_array(1,\@variableIdArr);
						#$insertSth->bind_param_array(1,11);
						$insertSth->bind_param_array(2,\@extractedValue);
						#$insertSth->bind_param_array(3,$self->{_timestamp_variables}); # NO ARRAY ONLY SCALAR
						$insertSth->bind_param_array(3, strftime "%a %b %e %H:%M:%S %Y", localtime); # NO ARRAY ONLY SCALAR
						$insertSth->bind_param_array(4,\@extraInfoArr);
						$insertSth->bind_param_array(5,\@execTableIdArr);
						$insertSth->bind_param_array(6,$outputId);
						$insertSth->bind_param_array(7,$imageId);
						$insertSth->execute_array({ArrayTupleStatus => \@tuple_status}) or print "\n ERROR IN INSERTING INTO OUPUT $!";
						print "\n the errors for pattern: ",$regexHash->{'id'}," and EXED ID: ",$self->{_regexContainer}->{'exec_id'}," the final insert result is: ", Dumper(@tuple_status) if ($verbose >= 2);
						$self->{_desdbh}->commit();
					}
				}
			}
		}
	}
	#print "\n the insert vals",scalar @insertVals ,Dumper(@insertVals);
        $insertSth->finish;
	
	close FH;
}


#sub getQAInfo {
#
#	my $self = shift;
#        my ($infoHashref) = @_;
#        my $getStatusHash;
#	my $sth;	
#	my $desjob_dbid;
#	my $sqlGetInfo = "Select threshold.intensity as status, out.qavariables_id from qa_output out, qaf_messages messages , qa_threshold threshold, execdefs where ";
#	my $row;
#	my $logFile = $infoHashref->{'logfile'};
#	open( my $logFileHandle, ">$logFile" );
#        if(defined $infoHashref->{'run'})
#        {
#		$sqlGetInfo .= ' block.run_id = '.$infoHashref->{'run'}.' and desjob.block_id = block.id and execdefs.desjob_id = desjob.id and execdefs.id = qa_output.execdefs_id and qaf_messages.pattern_id = qa_variables.id and qa_output.qavariables_id = qa_variables.id';
#        }
#
#	my $sqlGetStatus = "select count(threshold.intensity) as status_count, threshold.intensity as status, out.qavariables_id from qa_output out, qa_threshold threshold, execdefs  where execdefs.desjob_dbid = $desjob_dbid and out.execdefs_id = execdefs.id and out.qavariables_id = threshold.qavariables_id and out.value between threshold.min_value AND threshold.max_value group by out.qavariables_id, threshold.intensity";
#	$sth = $self->{_desdbh)->prepare($sqlGetInfo);
#	$sth->execute();
#	while($row = $sth->fetchrow_hashref()){
#		print $logFileHandle, $infoHashref->{'run'}."\t".$row->{'message'}.
#		
#	}
#        return $getStatusHash;
#
#
#}
#
####
## This subroutine cleans a filepath of the machine dependent part, and makes it compliant with DES file naming conventions.
###
sub stripFilePath {

	my ($self, $filePath) = @_;
	
	$filePath =~ s/^(.*?)\/Archive\///;
	return $filePath;
}


#
# Query the oracle sequencer for the location table
#
sub getNextOutputID {

 my ($self) = @_;

  my $outputId = 0;
  my $sql = qq{
      SELECT qaoutput_id.nextval FROM dual
  };

  my $sth=$self->{_desdbh}->prepare($sql);
  $sth->execute();
  $sth->bind_columns(\$outputId);
  $sth->fetch();
  $sth->finish();

  return $outputId;

}

###
# this function matches the exec table id given as a param to the controller with the exec id belonging to the pattern
###
sub execMatched {

	my ($self, $regexHash, $execTableId) = @_;
	print "\n matching exec $execTableId with ", $regexHash->{'exec_id'} if($verbose >=2);
	return 1 if($regexHash->{'exec_id'} == $execTableId || $regexHash->{'exec_id'} == 0);
}


sub getQAStatus {

	# support run, block, desjob, module, exec
	my $self = shift;
	my ($infoHashref) = @_;
	my $getStatusHash;
	if(defined $infoHashref->{'desjob_dbid'})
	{
		$getStatusHash = getStatusData($self, $infoHashref->{'desjob_dbid'});
	}

	return $getStatusHash;
}


sub getStatusData {

	my $self = shift;
	my ($infoHashref) = @_;
	my ($sqlFetchJobStatus,$fetchJobStatusSth,$rowFetchStatus,$varHash, $varThreshold,$retStatusArr, $sqlFetchStatMsg, @whereClause, @fromTables,$finalWhereForLookup,$finalFromTableForLookup);
	$finalFromTableForLookup = '';
	$finalWhereForLookup = '';

	push @fromTables, 'execdefs';
	if(defined $infoHashref->{'run'}){
		push @whereClause , ' run.run = \''.$infoHashref->{'run'}.'\' and run.id = block.run_id and desjob.block_id = block.id and module.desjob_id = desjob.id and execdefs.module_id = module.id ';
		push @fromTables, 'run';
		push @fromTables, 'block';
		push @fromTables, 'desjob';
		push @fromTables, 'module';
	} elsif(defined $infoHashref->{'block'}){
	#if(defined $infoHashref->{'block'} && not defined $infoHashref->{'run'}){
		push @whereClause , ' block.id = '.$infoHashref->{'block_id'}.' and desjob.block_id = block.id and module.desjob_id = desjob.id and execdefs.module_id = module.id ';
		push @fromTables, 'block';
		push @fromTables, 'desjob';
		push @fromTables, 'module';
	} elsif(defined $infoHashref->{'desjob_dbid'}){
	#if(defined $infoHashref->{'desjob_dbid'} && not defined $infoHashref->{'block_id'} && not defined $infoHashref->{'run'}){
		push @whereClause , ' execdefs.desjob_dbid = '.$infoHashref->{'desjob_dbid'};
		#push @fromTables, 'desjob';
	} elsif(defined $infoHashref->{'module_id'}){
		push @whereClause , ' module.id = '.$infoHashref->{'module_id'}.' and execdefs.module_id = module.id ';
		push @fromTables, 'module';
	}
	elsif(defined $infoHashref->{'execdefs_id'}){
	
		push @whereClause , ' execdefs.id = '.$infoHashref->{'execdefs_id'} ;
	}
	else{
		print "\n No params given";
	}
	

	###
	# Loop through the arrays containing all the needed FROM tables and Where conditions. Add an 'and' in the end 
	# for Where conditions and a comma ',' for FROM tables string
	###

	foreach my $whereClause (@whereClause){
	
		$finalWhereForLookup .= $whereClause.' and ';
	}
	$finalWhereForLookup = substr $finalWhereForLookup,0,-4;
	if ($finalWhereForLookup ne ''){
	
		$finalWhereForLookup = $finalWhereForLookup.' and '
	}

	foreach my $fromTable (@fromTables){
		if( $finalFromTableForLookup !~ /$fromTable/i){
		$finalFromTableForLookup .= $fromTable.', ';
		}	
	}
	$finalFromTableForLookup = substr $finalFromTableForLookup,0,-2;
	if ($finalFromTableForLookup ne ''){
	
		$finalFromTableForLookup = $finalFromTableForLookup.','
	}

	
	$sqlFetchJobStatus = "select count(threshold.intensity) as status_count, threshold.intensity as status, out.qavariables_id from $finalFromTableForLookup qa_output out, qa_threshold threshold where $finalWhereForLookup out.execdefs_id = execdefs.id and out.qavariables_id = threshold.qavariables_id and out.value between threshold.min_value AND threshold.max_value group by out.qavariables_id, threshold.intensity";
	print "\n The get QA LEVEL query: $sqlFetchJobStatus " if($verbose >=2);
	#$sqlFetchJobStatus = "select out.qavariables_id, threshold.intensity as status from qa_output out, qa_threshold threshold where out.job_dbid = $desJob_dbId and out.qavariables_id = threshold.qavariables_id and out.value between threshold.min_value AND threshold.max_value";
	$fetchJobStatusSth = $self->{_desdbh}->prepare($sqlFetchJobStatus) ;
	$fetchJobStatusSth->execute();
	while($rowFetchStatus = $fetchJobStatusSth->fetchrow_hashref()){
		#$varStatus->{$rowFetchStatus->{'qavariables_id'}} = applyThreshold($rowFetchStatus);
		#$varHash->{$rowFetchStatus->{'qavariables_id'}} = $rowFetchStatus;
		push @$retStatusArr, $rowFetchStatus;
	}
	
	my $sqlFetchStatPatterns = 'select count(qaf_messages.pattern_id) as count ,qaf_messages.pattern_id, qa_patterns.type as status from '.$finalFromTableForLookup.' qaf_messages, qa_patterns where '.$finalWhereForLookup.' qa_patterns.type like \'%status%\' and qa_patterns.id = qaf_messages.pattern_id and execdefs.id = qaf_messages.execdefs_id  group by qaf_messages.pattern_id, qa_patterns.type';
	print "\n The get STATUS LEVEL query: $sqlFetchStatPatterns " if($verbose >=2);
	$fetchJobStatusSth = $self->{_desdbh}->prepare($sqlFetchStatPatterns) ;
	$fetchJobStatusSth->execute();
	while(my $rowFetchStatusRows = $fetchJobStatusSth->fetchrow_hashref()){
		#$varStatus->{$rowFetchStatus->{'qavariables_id'}} = applyThreshold($rowFetchStatus);
		#$varHash->{$rowFetchStatus->{'qavariables_id'}} = $rowFetchStatus;
		push @$retStatusArr, $rowFetchStatusRows;
	}


	return $retStatusArr;
}


sub applyThresholdRules {

	my $self = shift;
	my ($varHash) = @_;
	my ($varKey,$arrVarIds,$strVarIds,$sqlGetThreshold,$rowThreshold, $thresholdSth, $retHash);
	foreach $varKey (keys %$varHash){
		$strVarIds .= $varHash->{$varKey}{'qavariables_id'}.", ";
		#push (@$arrVarIds, $varHash->{$varKey}{'qavariables_id'});
	}

	$strVarIds = substr $strVarIds, 0, -2;	
	$sqlGetThreshold = "select min_value, max_value, qavariables_id, intensity from qa_threshold where qavariables_id IN (".$strVarIds.") order by intensity asc";
	$thresholdSth = $self->{_desdbh}->prepare($sqlGetThreshold);
	$thresholdSth->execute();
	while($rowThreshold = $thresholdSth->fetchrow_hashref()){
		
		if($varHash->{$rowThreshold->{'qavariables_id'}}->{'value'} >= $rowThreshold->{'min_value'} || $varHash->{$rowThreshold->{'qavariables_id'}}->{'value'} <= $rowThreshold->{'max_value'}){
			$retHash->{$rowThreshold->{'qavariables_id'}} = $rowThreshold->{'intensity'}
		} 
	}
	print "\n the ret hash are ", Dumper($retHash);;
}


sub storeQAFMessage {

	my ($self,$line,$execTableId,$patternId) = @_;
	my $sql;
	my $sqlSth;
	if(!defined $patternId) {
	$patternId = 0;	
	}
	$sql = "insert into qaf_messages values ($execTableId,\'$line\',$patternId,".$self->{_timestamp}.")";
	$sqlSth = $self->{_desdbh}->prepare($sql) or print "Error in preparing $!";
        $sqlSth->execute() or print "\n\t ### error -> $! ###";#logError($!);
}


sub logError {
	my ($errorMessage) = @_;
	print "\n########### ERROR IN QAF BEGIN ########";
	print "\n\t $errorMessage";
	print "\n########### ERROR IN QAF END ########";
}

sub parse {

	my ($line,$regexContainerObject) = @_;
	my $regexPattern;
	my $regexCompiled;
	my @matchedArr;
	my $varHashTemp	;
	
	foreach my $regexObj ($regexContainerObject->{regex_hash}) {
                        foreach my $regexHash (@$regexObj) { # Loop through the patterns
                        $regexPattern = $regexHash->{'pattern'};
                        $regexCompiled = qr/$regexPattern/sm;
                        ##### match the regular expression #####
                        @matchedArr = ($line =~ $regexCompiled);
		}
	}
}

sub gulp {
    my ($file, $count) = @_;
    my @lines;
    for (1..$count) {
       push @lines, scalar <$file>;
       last if eof $file;
    }
    return @lines;
}

sub registerQAEvent {

	my ($self,$regexHash, $matchedArr) = @_;
	my $sql_getAllQAVars = "select * from qa_variables where pattern_id = ".$regexHash->{'id'};
	
	#print "\n the regex caught is: "; #$regexObj->id;
        my $patternQAVars;
	my $patternDBHashRef;
	my $patternid = $regexHash->{'id'};
        my $patternSql = qq{select * from QA_VARIABLES where pattern_id = $patternid order by pattern_location };
        my $patternSth = $self->{_desdbh}->prepare($patternSql) or print "Error in preparing $!";
        $patternSth->execute();

	while($patternDBHashRef = $patternSth->fetchrow_hashref()){
	
        push@{$patternQAVars}, $patternDBHashRef;

        }
	
        $patternSth->finish();


	#print "\n the qa vars in register ", Dumper($patternQAVars);
}

sub DESTROY
{
	my ($self) = @_;
         $self->{_desdbh}->disconnect();;
	print "\n ### DESTROYING QAFRAMEWORK " if ($verbose >= 3 );
}

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

QCFramework - Perl extension for blah blah blah

=head1 SYNOPSIS

  use QCFramework;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for QCFramework, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head2 EXPORT

None by default.



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

A. U. Thor, E<lt>ankitc@localdomainE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 by A. U. Thor

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.


=cut
