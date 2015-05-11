########################################################################
#  $Id$
#
#  $Rev::                                  $:  # Revision of last commit.
#  $LastChangedBy::                        $:  # Author of last commit. 
#  $LastChangedDate::                      $:  # Date of last commit.
#
#  Author: 
#         Darren Adams (dadams@ncsa.uiuc.edu)
#
#  Developed at: 
#  The National Center for Supercomputing Applications (NCSA).
#
#  Copyright (C) 2007 Board of Trustees of the University of Illinois. 
#  All rights reserved.
#
#  DESCRIPTION:
#
#######################################################################
package QCF::DESUtil;
use strict;
use warnings;
use Exception::Class::DBI;
#use FindBin;
#use lib ("$FindBin::Bin/../lib/perl5","$FindBin::Bin/../lib");
use base qw(QCF::Util);

sub new {
  my ($this, %Args) = @_;
  my ($dbfile);
  my %ConnectArgs = (
    'db_type' => 'ORACLE', # remove this
    'attr' => {
      'AutoCommit' => 0,
      'RaiseError' => 0,
      'PrintError'  => 0,
      'HandleError' => Exception::Class::DBI->handler,
      'FetchHashKeyName' => 'NAME_lc',
    },
  );

  # initialize tag to db if the tag is absent
  unless (defined $Args{'tag'}){
	$Args{'tag'} = 'DB';
  } 
  if(defined $Args{'tag'}  && $Args{'tag'} eq ''){
  # initialize dbfile to what is passed in the dbfile argument
	$Args{'tag'} = 'DB';
  }
  $dbfile = $Args{'dbfile'};

  # set the db file and section arguments. if the db file can be read, assign it. assign whatever is present in the section argument (even if null)
  if (defined $dbfile){
	$ConnectArgs{'db_config_file'} = $dbfile if (-r $dbfile);
  }
  $ConnectArgs{'db_config_file_section'} = $Args{'section'};
  $ConnectArgs{'db_config_file_tag'} = $Args{'tag'};

  # Override Default connect args with anything passed in:
  if (%Args) {
    while ((my $k, my $v) = each %Args) {
      if ($k =~ /DBIattr/i) {
        while ((my $k2, my $v2) = each %$v) {
          $ConnectArgs{'attr'}->{$k2} = $v2;
        }
      }
      else {
        $ConnectArgs{$k} = $v;
      }
    }
  }

  my $class = ref($this) || $this;
  my $self = $class->SUPER::new(%ConnectArgs);
  return $self;
}

package QCF::DESUtil::db;
use Exception::Class::DBI;
use Data::Dumper;
use strict;
use warnings;
use base qw(QCF::Util::db);

my %ClassData = ();

sub _init{
	my ($this, %Args) = @_;
}

# Accessor methods for important table data:
sub LOCATION_TABLE {
  my($self, $newvalue) = @_;
  $ClassData{'tables'}->{'LOCATION_TABLE'} = $newvalue if (@_ > 1);
  return $ClassData{'tables'}->{'LOCATION_TABLE'};
}

sub IMAGE_META_TABLE {
  my($self, $newvalue) = @_;
  $ClassData{'tables'}->{'IMAGE_META_TABLE'} = $newvalue if (@_ > 1);
  return $ClassData{'tables'}->{'IMAGE_META_TABLE'};
}

sub EXPOSURE_META_TABLE {
  my($self, $newvalue) = @_;
  $ClassData{'tables'}->{'EXPOSURE_META_TABLE'} = $newvalue if (@_ > 1);
  return $ClassData{'tables'}->{'EXPOSURE_META_TABLE'};
}

sub CATALOG_META_TABLE {
  my($self, $newvalue) = @_;
  $ClassData{'tables'}->{'CATALOG_META_TABLE'} = $newvalue if (@_ > 1);
  return $ClassData{'tables'}->{'CATALOG_META_TABLE'};
}

sub COADD_META_TABLE {
  my($self, $newvalue) = @_;
  $ClassData{'tables'}->{'COADD_META_TABLE'} = $newvalue if (@_ > 1);
  return $ClassData{'tables'}->{'COADD_META_TABLE'};
}

sub ZEROPOINT_TABLE {
  my($self, $newvalue) = @_;
  $ClassData{'tables'}->{'ZEROPOINT_TABLE'} = $newvalue if (@_ > 1);
  return $ClassData{'tables'}->{'ZEROPOINT_TABLE'};
}


sub mergeTmpTablePrc {
	my $dbh = shift;
	my $Args = shift;

	my ($prcName, $sourceTable, $destTable, $sql);
	my ($sourceSchema, $destSchema, $partName, $partValue, $overwrite);
	$sourceTable  = $Args->{'source_table'}  if($Args->{'source_table'});
	$destTable    = $Args->{'dest_table'}    if($Args->{'dest_table'});

    $sourceSchema = ($Args->{'source_schema'}) ? $Args->{'source_schema'} : "PIPELINE";
	$destSchema   = ($Args->{'dest_schema'})   ? $Args->{'dest_schema'}   : "DES_ADMIN";
	$partName     = ($Args->{'part_name'})     ? $Args->{'part_name'}     : $sourceTable;
	$partValue    = ($Args->{'part_value'})    ? $Args->{'part_value'}    : $sourceTable;
	$overwrite    = ($Args->{'overwrite'})     ? $Args->{'overwrite'}     : 0;

	$sql = qq{
	BEGIN DES_ADMIN.pMergeObjects(
		sourceTable=>'$sourceTable',
		destTable=>'$destTable',
		sourceSchema=>'$sourceSchema',
		destSchema=>'$destSchema',
		partName=>'$partName',
		partValue=>'$partValue',
		overwrite=>$overwrite
		);
	END;
	};
    my $start = time;
    my $sth = $dbh->do($sql);
    print "Merge time ", time-$start, " seconds\n";
  
	$dbh->commit();
}


################################################################################
################################################################################
sub mergeTmpTable {
  my $this = shift;
  my $Args = shift;

  my ($tmp_table, $obj_table, $partition_name, $drop, $tmp_table_index);
  $tmp_table_index = $Args->{'source_drop_index'} if ($Args->{'source_drop_index'});
  $tmp_table = $Args->{'source_table'} if ($Args->{'source_table'});
  $obj_table = $Args->{'target_table'} if ($Args->{'target_table'});
  $partition_name = $Args->{'partition_name'} if ($Args->{'partition_name'});
  $drop = $Args->{'drop'} if ($Args->{'drop'});

  my $PKey_name='K'.$tmp_table;

  my @dbq;

  if ($this->verbose() >=1) {
    print "\nmergeTmpTable: creating partition $partition_name and merging $tmp_table to $obj_table.\n";
  }

  if ($tmp_table_index) {
      # check if index exists in db
      my ( $count ) = $this->selectrow_array( "select count(*) from dba_indexes where index_name = '$tmp_table_index'" );
      if ($count > 0) {
          print "Found index on tmp table.   Dropping it.\n";
          push(@dbq, "DROP INDEX $tmp_table_index");
      }
      else {
          print "Did not find index on tmp table.\n";
      }
  }

#  push(@dbq,"alter table $tmp_table add constraint $PKey_name primary key (object_id) disable validate");

  push(@dbq,"alter table $obj_table add partition $partition_name values ('$tmp_table')");
  push(@dbq,"alter table des_admin.$obj_table exchange partition $partition_name with table $tmp_table including indexes without validation update global indexes");
  if ($drop) {
    push(@dbq,"DROP TABLE $tmp_table PURGE");
  }

  foreach my $dbq (@dbq) {
    print "$dbq\n";
    my $start = time;
    my $stat = $this->do($dbq);
    print "\t$stat\n";
    print "\tTime ", time-$start, " seconds\n";
  }
  $this->commit();
  print "\n";
}

################################################################################
## SUBROUTINE: queryDB
#################################################################################
sub queryDB {
    my $this = shift;
    my $Args = shift;

    if (exists($Args->{'table'}) && ($Args->{'table'} =~ /location/i)) {
        if (exists($Args->{'key_vals'}) && exists($Args->{'key_vals'}->{'detector'})) {
            if (exists($Args->{'key_vals'}->{'run'})) {
                print "Warning - desDBI::queryDB both detector and run exist in key_vals.\n";
                print "          detector is part of run in location table.\n";
                print "          Leaving run the same in key_vals\n";
                print "          deleting detector from key_vals\n"; 
                delete($Args->{'key_vals'}->{'detector'});
            }
            else {
                print "Warning - desDBI::queryDB detector exists in key_vals.\n";
                print "          detector is part of run in location table.\n";
                print "          creating run in key_vals with value '%_<detector>'\n";
                print "          deleting detector from key_vals\n";
                $Args->{'key_vals'}->{'run'} = "%_".$Args->{'key_vals'}->{'detector'};
                delete($Args->{'key_vals'}->{'detector'});
            }
        }
    }
    return $this->SUPER::queryDB($Args);
}

################################################################################
## SUBROUTINE: queryDB2
#################################################################################
sub queryDB2 {
    my ($this, %Args) = @_;

    # Modify table names here.  The table names that are part of this class
    # definition can be used in both the keys to the queryDB2 argument hash
    # (table names) and the "join" argument, and will be substituted with the
    # correct corresponding DB table name befor being passed to the queryDB2
    # in the parent class.
    my %NewArgs;
    foreach my $arg (keys %Args) {
      if (exists $ClassData{'tables'}->{$arg}) {
         my $key; 
         if ($ClassData{'tables'}->{$arg}->{'table_name'}) {
            $key = $ClassData{'tables'}->{$arg}->{'table_name'};
            $NewArgs{"$key"} = $Args{"$arg"};
          }
          if (exists $NewArgs{"$key"}->{'join'}) {
            foreach my $table_key (keys %{$ClassData{'tables'}}) {
              my $table_name = ClassData{'tables'}->{$table_key}->{'table_name'};
              $NewArgs{"$key"}->{'join'} =~ s/$table_key/$table_name/;
            }
          }   
      }
      else {
        $NewArgs{$arg} = $Args{$arg};
      }
    }

   #print "\nOriginal Args:\n",Dumper (\%Args),"\n";
   #print "\nNew Args:\n",Dumper (\%NewArgs),"\n";

    foreach my $table (keys %NewArgs) {

        if ($table =~ /location/i) {

            if (exists($NewArgs{$table}->{'key_vals'}) && exists($NewArgs{$table}->{'key_vals'}->{'detector'})) {
                if (exists($NewArgs{$table}->{'key_vals'}->{'run'})) {
                    print "Warning - desDBI::queryDB both detector and run exist in key_vals.\n";
                    print "          detector is part of run in location table.\n";
                    print "          Leaving run the same in key_vals\n";
                    print "          deleting detector from key_vals\n";
                    delete($NewArgs{$table}->{'key_vals'}->{'detector'});
                }
                else {
                    print "Warning - desDBI::queryDB detector exists in key_vals.\n";
                    print "          detector is part of run in location table.\n";
                    print "          creating run in key_vals with value '%_<detector>'\n";
                    print "          deleting detector from key_vals\n";
                    $NewArgs{$table}->{'key_vals'}->{'run'} = "%_".$NewArgs{$table}->{'key_vals'}->{'detector'};
                    delete($NewArgs{$table}->{'key_vals'}->{'detector'});
                }
            }
            last;
        }
        else {
            next;
        }
    }

    return $this->SUPER::queryDB2(%NewArgs);
} 


################################################################################
# SUBROUTINE: getDBTableInfo
# 
# To make this generic actually look up the primary key from the DB 
# rather than hard coding what we know it to be
################################################################################
sub getDBTableInfo {
   my $desdb = shift;
   my $tablestr = shift;
   my $RowsRef = shift;
    
   my $pkey; 
   $tablestr =~ tr/A-Z/a-z/;
   if ($tablestr =~ m/site/) {
      $tablestr = "SITES";
      $pkey = 'SITE_NAME';
   }
   elsif ($tablestr =~ m/archive/) {
      $tablestr = "ARCHIVE_SITES"; 
      $pkey = 'LOCATION_NAME';
   }
   elsif ($tablestr =~ m/software/) {
      #$tablestr = "SOFTWARE_LOCATIONS"; 
      #$pkey = 'LOCATION_NAME';
      $pkey = '';
      $tablestr = undef;
   }
   else {
      # calling program expected to figure out if this is a problem or not
      $pkey = '';
      $tablestr = undef;
   }

   if ($tablestr) {
     my $dbq = <<STR;
  SELECT * FROM $tablestr
STR

    @$RowsRef = @{$desdb->selectall_arrayref($dbq,{Slice => {}})};
  }

  return lc($pkey);
}


package QCF::DESUtil::st;
our @ISA = qw(QCF::Util::st);

1;

