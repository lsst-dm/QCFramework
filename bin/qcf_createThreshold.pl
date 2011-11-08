#!/usr/bin/perl -w


use strict;
use warnings;
use FileHandle;
use Getopt::Long;
use Data::Dumper;
use DB::DESUtil;

        my $patternHash;
	my ($fileList,$stdinBuffer,$line,$infoHashref,$node);
	my ($execdefs_id,$project,$run,$desjob_id);
	my (@allVars, $variableHash);


#
# Make a database connection
#
my $desdbh = DB::DESUtil->new();

my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime(time);
my $date;
$year += 1900;

if (length($mon) == 1){

	#$mon = "0".$mon;
}
######################### PATTERN CONFIG #######################
my $patternId = getnextId('qa_threshold',$desdbh);
$patternHash->{'id'} = $patternId;
$patternHash->{'pattern'} = "'STATUS2BEG\s*Image\s*(\/.*\.\w+)\s*\:\s*band\=(\w?)\s*ZP\=\s*(\d+\.?\d+)\s*STATUS2END'";
$patternHash->{'valid'} = "'y'";
$patternHash->{'timestamp'} = "to_date('$yday-$year', 'DDD-yyyy')";
$patternHash->{'type'} = "'qa'";
$patternHash->{'exec_id'} = 0;


# 32,40:s/\n/\r#/gc use this template to comment out all the lines with variables in them. replace 32,40 with start and end line numbers between which the comments are to be introduced.

######################### VARIABLE CONFIG #######################

$variableHash->{'id'} = getnextId('qa_variables',$desdbh);
$variableHash->{'name'} = "'Image'";
$variableHash->{'pretty_name'} = "'Image'";
$variableHash->{'pattern_id'} = $patternId;
$variableHash->{'pattern_location'} = 1;
$variableHash->{'valid'} = "'y'";
$variableHash->{'timestamp'} = "to_date('$yday-$year', 'DDD-yyyy')";
$variableHash->{'action_code'} = 1;

push @allVars, $variableHash;

undef $variableHash;
$variableHash->{'id'} = getnextId('qa_variables',$desdbh);
$variableHash->{'name'} = "'Band'";
$variableHash->{'pretty_name'} = "'Coadd Catalog Band'";
$variableHash->{'pattern_id'} = $patternId;
$variableHash->{'pattern_location'} = 2;
$variableHash->{'valid'} = "'y'";
$variableHash->{'timestamp'} = "to_date('$yday-$year', 'DDD-yyyy')";
$variableHash->{'action_code'} = 0;

push @allVars, $variableHash;
undef $variableHash;

$variableHash->{'id'} = getnextId('qa_variables',$desdbh);
$variableHash->{'name'} = "'ZP'";
$variableHash->{'pretty_name'} = "'Coadd Catalog ZP'";
$variableHash->{'pattern_id'} = $patternId;
$variableHash->{'pattern_location'} = 3;
$variableHash->{'valid'} = "'y'";
$variableHash->{'timestamp'} = "to_date('$yday-$year', 'DDD-yyyy')";
$variableHash->{'action_code'} = 0;


push @allVars, $variableHash;

######################### VARIABLE CONFIG #######################


my $sqlPattern = 'insert into qa_patterns values ('.$patternHash->{'id'}.','.$patternHash->{'pattern'}.','.$patternHash->{'valid'}.','.$patternHash->{'timestamp'}.','.$patternHash->{'type'}.','.$patternHash->{'exec_id'}.')';
	print "\n the sql variable $sqlPattern ";
my $sthPattern = $desdbh->prepare($sqlPattern);
$sthPattern->execute() or print 'cannot insert into qa_patterns';
$desdbh->commit();

print "\n the array ",Dumper(@allVars);


if(scalar @allVars > 0)
{
	my $sqlVariables;# = 'insert into qa_variables values ';
	my $sthVariables; # = $desdbh->prepare($sqlVariables);

	foreach my $varHashTemp (@allVars){

	$sqlVariables = ' insert into qa_variables values  ('.$varHashTemp->{'id'}.','.$varHashTemp->{'name'}.','.$varHashTemp->{'pretty_name'}.','.$varHashTemp->{'pattern_id'}.','.$varHashTemp->{'pattern_location'}.','.$varHashTemp->{'valid'}.','.$varHashTemp->{'timestamp'}.','.$varHashTemp->{'action_code'}.') ';

	print "\n\n the sql variable $sqlVariables ",Dumper($varHashTemp);
	$sthVariables = $desdbh->prepare($sqlVariables);
	$sthVariables->execute() or print "cannot insert into qavariables";
	$desdbh->commit();

	}

	#$sqlVariables = substr $sqlVariables,0,-2;
}

$desdbh->disconnect();

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
        "\n"
          . "usage: $command "
          . " -execdefs_id ExecDefsTableId -project ProjectNameString -run RunNumber -desjob_id DESJobSubmitId \n"
          . "       DESJobIdUniqueDBId is the unique DB id associated in the DB with every DB Job.\n"
          . "       if desjob_dbid is NOT known, Jobs can be identified by providing a bunch of other variables together:\n"
          . "          project: The name of the project 'DES', run: The run number, desjob_id: The submit ID for jobs\n"
    );

    die("\n")

}

