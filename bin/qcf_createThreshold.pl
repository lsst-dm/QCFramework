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

# 32,40:s/\n/\r#/gc use this template to comment out all the lines with variables in them. replace 32,40 with start and end line numbers between which the comments are to be introduced.

######################### VARIABLE CONFIG #######################

$variableHash->{'qavariables_id'} = 53; 
$variableHash->{'min_value'} = 500;
$variableHash->{'max_value'} = 5000;
$variableHash->{'valid'} = "'y'";
$variableHash->{'timestamp'} = "to_date('$yday-$year', 'DDD-yyyy')";
$variableHash->{'intensity'} = 4;
$variableHash->{'type'} = "'qa'";

push @allVars, $variableHash;

undef $variableHash;

$variableHash->{'qavariables_id'} = 54; 
$variableHash->{'min_value'} = 500;
$variableHash->{'max_value'} = 5000;
$variableHash->{'valid'} = "'y'";
$variableHash->{'timestamp'} = "to_date('$yday-$year', 'DDD-yyyy')";
$variableHash->{'intensity'} = 4;
$variableHash->{'type'} = "'qa'";

push @allVars, $variableHash;
undef $variableHash;

######################### VARIABLE CONFIG #######################


if(scalar @allVars > 0)
{
	my $sqlVariables;# = 'insert into qa_variables values ';
	my $sthVariables; # = $desdbh->prepare($sqlVariables);

	foreach my $varHashTemp (@allVars){

	$sqlVariables = ' insert into qa_threshold values  ('.$varHashTemp->{'qavariables_id'}.','.$varHashTemp->{'min_value'}.','.$varHashTemp->{'max_value'}.','.$varHashTemp->{'timestamp'}.','.$varHashTemp->{'valid'}.','.$varHashTemp->{'intensity'}.','.$varHashTemp->{'type'}.') ';

	print "\n\n the sql variable $sqlVariables ",Dumper($varHashTemp);
	$sthVariables = $desdbh->prepare($sqlVariables);
	$sthVariables->execute() or print "cannot insert into qathreshold";
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

