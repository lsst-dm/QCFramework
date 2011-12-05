#!/usr/bin/perl -w
package MyHandler;


use strict;
use warnings;
use FileHandle;
use QCF::QCFramework;
use Getopt::Long;
use Data::Dumper;
use DB::DESUtil;

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

use XML::Generator::DBI;
use XML::Handler::YAWriter;
use XML::SAX::Writer;
use XML::Parser::PerlSAX;
use FileHandle;

my $patternHash;
my ($current_element);
my ($filePath,$verbose);
my ($tableHash,$whereHash,@statusArr,$whereColArr,@tableColArr,@desjobIdArr,@runArr,$colArr,$parentTag,$currTable,$whereTableName,$whereColVal,@allTablesNeeded,$sqlWhereColumns,$sqlFrom,$sqlFinal,$infoHashref,@allSqlComponents,$sqlWhereFinal,$sqlWhereStatus,$sqlWhereDesjobid,$sqlWhereRun,$sqlSelect,@finalRes);

	
Getopt::Long::GetOptions(
    "filepath=s"     => \$filePath,
    "verbose:i"     => \$verbose,
) or usage("Invalid command line options\n");

usage("\nYou must provide the xml file path to proceed") unless defined $filePath;
	
my $desdbh = DB::DESUtil->new();
my $handler = MyHandler->new();
my $parser = XML::Parser::PerlSAX->new( Handler => $handler );
my %parser_args = (Source => {SystemId => $filePath});
my $result = $parser->parse(%parser_args);

	#$infoHashref->{'desjob_dbid'} = $desjob_dbid;
	#$infoHashref->{'project'} = $project;
	#$infoHashref->{'run'} = $run;
	#$infoHashref->{'desjob_id'} = $desjob_id;
	#$infoHashref->{'verbose'} = $verbose;

	#my $qcFramework = QCFramework->new($infoHashref);

	#print "\n ### $desjob_dbid";
	#my $statusHashRef = $qcFramework->getQCInfo($infoHashref);
	#print "\n the statusHashRef: ", Dumper($statusHashRef);
	
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
          . "usage: perl qcf_query.pl "
          . "-filepath pathToXMLFile \n"
          . "\tThe purpose of this script is to model queries to the QC Framework database\n "
          . "\tUse the sample XML file (query.xml) to create your own query XML File\n "
    );

    die("\n")

}


sub start_element {

my ($self, $element) = @_;
	$current_element = $element->{Name};
          switch($current_element)
                {
			case 'QUERY'{}
                        case 'TABLE'
                        {
                                # this tag means that we need columns from this table 
                                undef $tableHash;
				$parentTag = 'table';
                        }
                        case 'WHERE'
                        {
                                undef $whereHash;
				undef @desjobIdArr;
                                undef @statusArr;
				# this tag means the where conditions are starting
				$parentTag = 'where';
                        }
                        case 'STATUS'
                        {
                                #undef @statusArr;
				# this tag means that data with this status needs to be included in the result set
                        }
                        case 'DESJOBID'
                        {
                                #undef $desjobIdArr;
				# this tag contains the DESJOBIDs to be searched for. there could be more than one
                        }
                        case 'RUN'
                        {
				# this tag contains the RUNs to be searched for. there could be more than one
                        }
                        case 'WHERECOLUMN'
                        {
				undef $whereColArr;
				$parentTag = 'wherecol';
                        }
                        case 'COLUMN'
                        {
				undef $colArr;
				#comes with the table tags. it contains the columns which need to be included from a table
                        }
                        case 'NAME'
                        {
				# Name tag comes under the table tags and Wherecolumn tags. it contains the name of table/column 
				
                        }
                        case 'TABLENAME'
                        {
				#tablename is that tag under wherecolumn tag and it contains the name of the table from where the column needs to be inlcuded in the where clause
                        }
                        case 'VALUE'
                        {
				#value is a tag inside the wherecolumn tag. it contains the value of the column to be queried
                        }
                        else {
				die("Wrong tag $current_element please verify the xml file");
                        }
                }
}




sub trim
{
        my $string = shift;
        $string =~ s/^\s+//;
        $string =~ s/\s+$//;
        return $string;
}

sub characters {
    my ($self, $characters) = @_;
    my $text = $characters->{Data};
	
	chomp($text);
        if(trim($text) ne '')
        {
	switch($current_element)
                {
                        case 'TABLE'
                        {
                                # this tag means that we need columns from this table 
                        }
                        case 'WHERE'
                        {
                                # this tag means the where conditions are starting
                        }
                        case 'STATUS'
                        {
				push @statusArr, $text
                        }
                        case 'DESJOBID'
                        {
				push @desjobIdArr, $text
                        }
                        case 'RUN'
                        {
				push @runArr, $text;
                        }
                        case 'WHERECOLUMN'
                        {
                        }
                        case 'COLUMN'
                        {
				push @tableColArr,$text; 
				print "\n the case for COLUMN inserting --$text-- ";
                        }
                        case 'NAME'
                        {	
				# Name is the second level column after TABLE. this contains the name of the table which needs to be included
				$currTable = $text; #in case of wherecolumn tag, $currtable is a misnomer and it holds the name of the column instead of the name of the table		
                        }
                        case 'TABLENAME'
                        {
				$whereTableName = $text;
                        }
                        case 'VALUE'
                        {
				$whereColVal = $text;
                        }
                        else {
                        }
                }
	}
}


sub end_element {
    my ($self, $element) = @_;

	my  $end_element = $element->{'Name'};
#	print "\n the end element $end_element", Dumper($element);
	switch($end_element)
                {
			case 'QUERY' {
				foreach (@allTablesNeeded){
					#print "\n the from component ",Dumper($_);
					if($sqlFrom !~ /$_/){
					$sqlFrom .= "$_ , ";
					}
					#$sqlFrom .= "$_ , ";
				}
				foreach (@allSqlComponents){
					#print "\n the where component ", Dumper($_);
				
				$sqlWhereFinal .= $_." and "; 
				}
				$sqlWhereFinal = substr $sqlWhereFinal,0,-4;
				$sqlFrom = substr $sqlFrom,0,-2;
				$sqlFinal = "SELECT $sqlSelect FROM $sqlFrom WHERE $sqlWhereFinal";
				print "\n the final query $sqlFinal ";
				my $sth = $desdbh->prepare($sqlFinal);	
				$sth->execute() or print "\n Error in executing";
				while(my $row = $sth->fetchrow_hashref()){
					print Dumper($row);
					#push @finalRes,$row;	
				}
			
				#print Dumper(@finalRes);	
			}
                        case 'TABLE'
                        {
				push @allTablesNeeded, $currTable;

                                # this tag means that we need columns from this table 
				if ($sqlSelect ne ''){
				$sqlSelect .= ', ';
				}
				foreach (@tableColArr){
				
				$sqlSelect .= " ".$currTable.".".$_.", ";
				}
				$sqlSelect = substr $sqlSelect,0,-2;
				
				switch($currTable){
				
				case 'QC_OUTPUT'{
				}	
				case 'QCF_MESSAGES'{
				}	
				case 'QC_THRESHOLD'{
				}	
				case 'QC_VARIABLES'{
				}	
				case 'QC_PATTERNS'{
				}	
				case ''{
				}	
				}
				undef @tableColArr;
                        }
                        case 'WHERE'
                        {
				if(scalar @statusArr > 0){
				$sqlWhereStatus = " qc_threshold.intensity in (";

				foreach (@statusArr){
					$sqlWhereStatus .= $_.", ";	
				}

				$sqlWhereStatus = substr $sqlWhereStatus,0,-2;
				$sqlWhereStatus .= " )";

				$sqlWhereStatus .= " and qc_threshold.qcvariables_id = qc_output.qcvariables_id ";
				push @allSqlComponents, $sqlWhereStatus;
				push @allTablesNeeded, 'QC_THRESHOLD';
				}

				
				
				if(scalar @desjobIdArr > 0){
				$sqlWhereDesjobid = " execdefs.desjob_dbid in (";

				foreach (@desjobIdArr){
					$sqlWhereDesjobid .= $_.", ";	
				}

				$sqlWhereDesjobid = substr $sqlWhereDesjobid,0,-2;
				$sqlWhereDesjobid .= " )";

				$sqlWhereDesjobid .= " and execdefs.id = qc_output.execdefs_id";
				push @allSqlComponents, $sqlWhereDesjobid;
				push @allTablesNeeded, 'EXECDEFS';
				push @allTablesNeeded, 'QC_OUTPUT';

				}
			
				if(scalar @runArr > 0){
                                $sqlWhereRun = " block.run_id in (";
                                           
                                foreach (@runArr){
                                        $sqlWhereRun .= $_.", ";
                                }

                                $sqlWhereRun = substr $sqlWhereRun,0,-2;
                                $sqlWhereRun .= " )";

                                $sqlWhereRun .= " and desjob.block_id = block.id and execdefs.desjob_id = desjob.id and execdefs.id = qc_output.execdefs_id and qcf_messages.pattern_id = qc_variable.id and qc_output.qcvariables_id = qc_variable.id";
				push @allSqlComponents, $sqlWhereRun;
				push @allTablesNeeded, 'BLOCK';
                                } 
				
                                # this tag means the where conditions are starting
                        }
                        case 'STATUS'
                        {
                        }
                        case 'DESJOBID'
                        {
                        }
                        case 'RUN'
                        {
                        }
                        case 'WHERECOLUMN'
                        {
				$sqlWhereColumns = " $whereTableName.$currTable = $whereColVal";
				push @allSqlComponents, $sqlWhereColumns;
                        }
                        case 'COLUMN'
                        {
                        }
                        case 'NAME'
                        {	
                        }
                        case 'TABLENAME'
                        {
                        }
                        case 'VALUE'
                        {
                        }
                        else {
                        }
		}
}

sub start_document {
    my ($self) = @_;
    #print "Starting SAX \n";
}

sub end_document {
    my ($self) = @_;
#print "done with the doc";
}


sub new {
    my $type = shift;
    return bless {}, $type;
}









