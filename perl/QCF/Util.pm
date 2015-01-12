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
package QCF::Util;
use strict;
use warnings;
#use FindBin;
#use lib ("$FindBin::Bin/../lib/perl5","$FindBin::Bin/../lib");
use base qw(QCF::Connection);



package QCF::Util::db;
use Exception::Class::DBI;
use Data::Dumper;
use Carp;
use strict;
use warnings;
our @ISA = qw(QCF::Connection::db);

#######################################################################
#  createTableAs
#  
#  INPUT:
#    Single hashref of arguments:
#      'table_name'  - Name of new table
#      'as'          - Name of existing table to mimic
#      'existok'     - 
#      'temp'        - 1 or 0; make this a true TEMP table
#      'tablespace'  - Create table in this Tablespace
#
#  DESCRIPTION:
#    Create a new database table that shares its' entire structure with
#    another existing table.  This function is written with exception 
#    handling that depends on Exception::Class::DBI.
#
#######################################################################
sub createTableAs {
  my $dbh = shift;
  my $Args = shift;  # pass a hashref of arguments:
  
  my $tableName = $Args->{'table_name'} if ($Args->{'table_name'});
  my $AsTableName = $Args->{'as'} if ($Args->{'table_name'});
  my $existok = $Args->{'existok'} if ($Args->{'existok'});
  my $tempTable = $Args->{'temp'} if ($Args->{'temp'});
  my $tablespace = $Args->{'tablespace'} if ($Args->{'tablespace'});
  
  croak("Must provide a table name.\n") if !($tableName);
  croak("Must provide a table name to created new table \"AS\".\n") if !($AsTableName);
  
  print "Creating $tableName using $AsTableName as a template.\n";
 
  my $sql; 
  if($tempTable){
    $sql= "CREATE GLOBAL TEMPORARY TABLE $tableName ON COMMIT DELETE ROWS AS SELECT * FROM $AsTableName WHERE 0=1";
  }
  else {
    $sql= "CREATE TABLE $tableName NOLOGGING ";
    if($tablespace){
	$sql= "$sql TABLESPACE $tablespace ";
    }
    $sql= "$sql AS SELECT * FROM $AsTableName WHERE 0=1";
  }

  eval {
  my $sth = $dbh->do($sql);
  $sth->finish();
  };
  
  if (my $e =  Exception::Class::DBI->caught()) {
    if ($e->err == 955 && $existok) {
      warn "Table: $tableName exists continuing...\n";
    }
    else {
      print STDERR "\nDBI Exception:\n"; 
      print STDERR "  Exception Type: ", ref $e, "\n";
      print STDERR "  Err:            ", $e->err, "\n";
      #print STDERR "  Error:\n", $e->error, "\n\n";
      $e->rethrow;
    }
  
  }

}


################################################################################
# SUBROUTINE: queryDB
# Arguments:
#   A single hash(ref) with the following parameters:
#      'table' - Name of database table.
#      'key_vals' - Data to define the WHERE clause (optional).
#      'where' - Specified where argument (optional).
#      'select_fields' - Arrayref of field names to be selected (optional).
################################################################################
sub queryDB {
  my $this = shift;
  my $Args = shift;

  my ($where,$table,$dbq,$fields,$select,$Rows,@where_statements,$FKeys,@FKeys,@bind_keys);

  local * bail = sub {
    my $message = shift;
    croak (
      qq($message\n),
      qq(Expected Arguments:\n),
      qq(  'table' => string\n),
      qq(  'key_vals' => hashref OPTIONAL\n),
      qq(  'where => string OPTIONAL\n),
      qq(  'select_fields' => arrayref OPTIONAL\n\n)
    );

  };

  #########################################################
  #
  # Process arguments
  #
  #########################################################

  # Die if no table is provided:
  if ($Args->{'table'}) {
    $table = $Args->{'table'};
  }
  else {
    bail('Must specify a table name');
  }

  # Funky where arguments can also be specified:
  if ($Args->{'where'}) {
    @where_statements = @{$Args->{'where'}};
  }

  # List of key_vals for where clause:
  if (exists $Args->{'key_vals'}) {
    $FKeys = $Args->{'key_vals'};
    @FKeys = keys %{$Args->{'key_vals'}};
  }

  # Get actual column names for the provided table:
  my $cols = $this->getColumnNames($table);

  #########################################################
  #
  # Build the SQL
  #
  #########################################################


  # Setup the SELECT part of the sql statement, grab all columns (*) 
  # unless some are provided:
  if ($Args->{'select_fields'}) {
    foreach my $field ( @{$Args->{'select_fields'}} ) {
      if (exists $cols->{lc($field)}) {
        if ($fields) {
          $fields .= ','.$field;
        }
        else {
          $fields = $field;
        }
      }
      else {
        bail("The $field column does not exist in the $table table.");
      }
    }
    $select = "SELECT $fields FROM $table";
  }
  else {
    $select = "SELECT * FROM $table";
  }



  # Turn key_vals information into a where clause.  If any value is an arrayref
  # it gets pushed into and array of keys that will be parameter bound and looped over
  # for each value in the array when the query is executed below.
  if ($FKeys) {
    foreach my $key (@FKeys) {
      if (! exists $cols->{lc($key)}) {
        bail("The $key column does not exist in the $table table.");
      }
      my $val = $FKeys->{"$key"};
      # Do parameter bind and multiple executes for large lists.  For
      # small lists simply add 'IN' clause to the where statement
      if (ref $val eq 'ARRAY') {
        if (scalar @$val > 50) {
          push(@bind_keys, $key);
          if ($where) {
            $where = $where." AND $key=?";
          }
          else {
            $where = "WHERE $key=?";
          }
        }
        else {
          if ($where) {
            $where = $where." AND $key IN (".join(',', map{$this->quote($_)} @$val ).")";
          }
          else {
            $where = "WHERE $key IN (".join(',', map{$this->quote($_)} @$val ).")";
          }
        }
      }
      else {
        # REGEXP_LIKE:
        if ($val =~ /[\*\^\$\[\]\&]/) {
           if ($where) {
            $where = $where." AND REGEXP_LIKE($key,'$val')";
          }
          else {
            $where = "WHERE REGEXP_LIKE($key,'$val')";
          }
        }
        # LIKE:
        elsif ($val =~ /\%/ && $val !~ /!/) {
          if ($where) {
            $where = $where." AND ".$key.' LIKE '.$this->quote($val);
          }
          else {
            $where = "WHERE ".$key.' LIKE '.$this->quote($val);
          }
          if ($val =~ /\\/) {
            $where .= " ESCAPE '\\'";
          }
        }
        # NOT LIKE: 
        elsif ($val =~ /\%/ && $val =~ /!/){
          $val =~ s/!//;
          if ($where) {
            $where = $where." AND ".$key.' NOT LIKE '.$this->quote($val);
          }
          else {
            $where = "WHERE ".$key.' NOT LIKE '.$this->quote($val);
          }
          if ($val =~ /\\/) {
            $where .= " ESCAPE '\\'";
          }
        }
        # Not equal:
        elsif ($val =~ /!/ && $val !~ /\%/) {
          $val =~ s/!//;
          if ($where) {
            $where = $where." AND $key!=".$this->quote($val);
          }
          else {
            $where = "WHERE $key!=".$this->quote($val);
          }
        }
        # Equal:
        else {
          if ($where) {
            $where = $where." AND $key=".$this->quote($val);
          }
          else {
            $where = "WHERE $key=".$this->quote($val);
          }
        }
      }
    }
  }
  
  # Add completly specified where arguments:
  if (@where_statements) {
    foreach my $clause (@where_statements) {
      if ($where) {
        $where = $where." AND ".$clause;
      }
      else {
        $where = "WHERE ".$clause;
      }
    }
  
  }
  
  # The rest of the sql...
  if ($where) {
    $dbq = join(' ',$select,$where);
  }
  else { 
    $dbq = $select;
  }
  
  #########################################################
  #
  # Execute the SQL and get result data set.
  #
  #########################################################

  my $sth = $this->prepare($dbq);

  if ($this->verbose() >= 2) {
    print "\nExecuting: $dbq\n";
  }



  if (@bind_keys) {
    my $N_bound_fields = $#bind_keys;
    bail("Only one multi-valued field is currently supported") if ( $N_bound_fields > 0);
    #foreach my $bound_field (@bind_keys) {
      my $bound_field = $bind_keys[0];
      my $values = $Args->{'key_vals'}->{"$bound_field"};
      my $N_values = $#$values;

#      # Expecting only one row in each call:
#      foreach (my $i=0; $i<=$N_values; $i++) {
#        $sth->execute($values->[$i]);
#        $Rows->[$i] = $sth->fetchrow_hashref;
#      }

      # Expecting multiple rows in one call: 
      foreach (my $i=0; $i<=$N_values; $i++) {
        $sth->execute($values->[$i]);
        my $Set =  $sth->fetchall_arrayref({});
        push(@$Rows,@$Set);  # Seems like this might suck.
      }


    #}


  }


  else {
    $sth->execute();
    $Rows = $sth->fetchall_arrayref({});
  }
  $sth ->finish();
  $this->commit();
  
  my $nrows = $#$Rows + 1;
  if ($this->verbose() >= 2) {
    if ($nrows == 1) {
      print "Selected $nrows row.\n";
    }
    else {
      print "Selected $nrows rows\n";
    }
  }
  
  return $Rows;
}

################################################################################
# SUBROUTINE: queryDB2
#
# Second generation SQL builder.
#
# Arguments:
#   A single hash(ref) with the following parameters:
#      'table' - Name of database table.
#      'key_vals' - Data to define the WHERE clause (optional).
#      'where' - Specified where argument (optional).
#      'select_fields' - Arrayref of field names to be selected (optional).
################################################################################
sub queryDB2 {
  my ($this, %Args) = @_;

  if ($this->debug) {
    print "\nQueryDB2 arguments:\n";
    print Dumper(\%Args),"\n";
  }

  my (@bind_keys,$Rows);


  my $query = $this->prepareQueryDB2(%Args);
  my $sth = $query->{'statement_handle'};
  if ($query->{'bind_list'} && scalar @{$query->{'bind_list'}} > 0) {
    @bind_keys = @{$query->{'bind_list'}};
  }

  if ($this->verbose() >= 2 || $this->debug()) {
    print "\nExecuting: $sth->{'Statement'}\n";
  }

  if (@bind_keys) {
    my $N_bound_fields = $#bind_keys;
    bail("Only one multi-valued field is currently supported") if ( $N_bound_fields > 0);
    #foreach my $bound_field (@bind_keys) {
      my $bound_field = $bind_keys[0];
      my ($t,$k) = split(/\./,$bound_field);
      my $values = $Args{$t}->{'key_vals'}->{$k};
      my $N_values = $#$values;

#      # Expecting only one row in each call:
#      foreach (my $i=0; $i<=$N_values; $i++) {
#        $sth->execute($values->[$i]);
#        $Rows->[$i] = $sth->fetchrow_hashref;
#      }

      # Expecting multiple rows in one call: 
      foreach (my $i=0; $i<=$N_values; $i++) {
        $sth->execute($values->[$i]);
        if ($Args{'hash_key'}) {
          my $Set = $sth->fetchall_hashref($Args{'hash_key'});
          if ($Rows) {
            $Rows = {%$Rows,%$Set};
          }
          else {
            $Rows = $Set;
          }
        }
        else {
          my $Set =  $sth->fetchall_arrayref({});
          push(@$Rows,@$Set);  # Seems like this might suck.
        }
      }


    #}


  }


  else {
    $sth->execute();
    if ($Args{'hash_key'}) {
      $Rows = $sth->fetchall_hashref($Args{'hash_key'});
    }
    else {
      $Rows = $sth->fetchall_arrayref({});
    }
  }
  $sth ->finish();
  $this->commit();
  
  my $nrows;
  if (ref $Rows eq 'ARRAY') {
    $nrows = scalar @$Rows;
  }
  elsif (ref $Rows eq 'HASH') {
    $nrows = scalar keys %$Rows;
  }
  if ($this->verbose() >= 2) {
    if ($nrows == 1) {
      print "Selected $nrows row.\n";
    }
    else {
      print "Selected $nrows rows\n";
    }
  }
  
  return $Rows;
}


################################################################################
# SUBROUTINE: prepareQueryDB2
################################################################################
sub prepareQueryDB2 {
  my ($this, %Args) = @_;


  local * bail = sub {
    my $message = shift;
    croak (
      qq($message\n),
      qq(Expected Arguments:\n),
      qq(  'table' => string\n),
      qq(  'key_vals' => hashref OPTIONAL\n),
      qq(  'where => string OPTIONAL\n),
      qq(  'select_fields' => arrayref OPTIONAL\n\n)
    );

  };

  #########################################################
  #
  # Process arguments
  #
  #########################################################

  my ($dbq,$fields,$Rows,@bind_keys);
  my ($select, $from, $where);

  while ((my $table, my $Args) = each %Args) {
    next if (ref $Args ne 'HASH');
    my (@where_statements,$FKeys,@FKeys);
    $fields = undef;

  # Check existance of each table here?
  

    # Funky where arguments can also be specified:
    if ($Args->{'where'}) {
      @where_statements = @{$Args->{'where'}};
    }

    # List of key_vals for where clause:
    if (exists $Args->{'key_vals'}) {
      $FKeys = $Args->{'key_vals'};
      @FKeys = keys %{$Args->{'key_vals'}};
    }

    # Get actual column names for the provided table, from the DB:
    my $cols = $this->getColumnNames($table);

    # Setup the SELECT part of the sql statement, grab all columns (*) 
    # unless some are provided:
    if (exists $Args->{'select_fields'}) {
      if (ref $Args->{'select_fields'} eq 'ARRAY') {
        foreach my $field ( @{$Args->{'select_fields'}} ) {
          if (exists $cols->{lc($field)}) {
            if ($fields) {
              $fields .= ",$table\.$field";
            }
            else {
              $fields = "$table\.$field";
            }
          }
          else {
            bail("The $field column does not exist in the $table table.");
          }
        }
      }
      elsif (lc($Args->{'select_fields'}) eq 'all') {
        if ($select) {
          $select .= ",$table\.*"; 
        }
        else {
          $select = "SELECT $table".'.*';
        }
      }
      if ($fields) {
        if ($select) {
          $select = $select.','.$fields;
        }
        else {
          $select = "SELECT $fields";
        }
      }
    }
#    else {
#      if ($select) {
#        $select .= ",$table\.*"; 
#      }
#      else {
#        $select = "SELECT $table".'.*';
#      }
#    }

    if ($from) {
      $from .= ','.$table;
    }
    else {
      $from = "FROM $table";
    }


    # Turn key_vals information into a where clause.  If any value is an arrayref
    # it gets pushed into and array of keys that will be parameter bound and looped over
    # for each value in the array when the query is executed below.
    if ($FKeys) {
      foreach my $key (@FKeys) {
        if (! exists $cols->{lc($key)}) {
          bail("The $key column does not exist in the $table table.");
        }
        my $val = $FKeys->{"$key"};
        # Do parameter bind and multiple executes for large lists.  For
        # small lists simply add 'IN' clause to the where statement
        my $multi = 0;
        if (ref $val eq 'ARRAY') {
          $multi = 1 if (scalar @$val > 1);
        }
        if ($multi) {
          if (scalar @$val > 50) {
            push(@bind_keys, "$table\.$key");
            if ($where) {
              $where = $where." AND ".$table.'.'."$key=?";
            }
            else {
              $where = "WHERE ".$table.'.'."$key=?";
            }
          }
          else {
            if ($where) {
              $where = $where." AND ".$table.'.'."$key IN (".join(',', map{$this->quote($_)} @$val ).")";
            }
            else {
              $where = "WHERE ".$table.'.'."$key IN (".join(',', map{$this->quote($_)} @$val ).")";
            }
          }
        }
        else {
          my $str_val;
          if (ref $val eq 'ARRAY') {
            $str_val = $val->[0];
          }
          else {
            $str_val = $val;
          }
          # REGEXP_LIKE:
          if ($str_val =~ /[\*\^\$\[\]\&]/) {
             if ($where) {
              $where = $where." AND REGEXP_LIKE($table\.$key,'$str_val')";
            }
            else {
              $where = "WHERE REGEXP_LIKE($table\.$key,'$str_val')";
            }
          }
          # LIKE:
          elsif ($str_val =~ /\%/ && $str_val !~ /!/) {
            if ($where) {
              $where = $where." AND ".$table.'.'.$key.' LIKE '.$this->quote($str_val);
            }
            else {
              $where = "WHERE ".$table.'.'.$key.' LIKE '.$this->quote($str_val);
            }
            if ($str_val =~ /\\/) {
              $where .= " ESCAPE '\\'";
            }
          }
          # NOT LIKE: 
          elsif ($str_val =~ /\%/ && $str_val =~ /!/){
            $str_val =~ s/!//;
            if ($where) {
              $where = $where." AND ".$table.'.'.$key.' NOT LIKE '.$this->quote($str_val);
            }
            else {
              $where = "WHERE ".$table.'.'.$key.' NOT LIKE '.$this->quote($str_val);
            }
            if ($str_val =~ /\\/) {
              $where .= " ESCAPE '\\'";
            }
          }
          # Not equal:
          elsif ($str_val =~ /!/ && $str_val !~ /\%/) {
            $str_val =~ s/!//;
            if ($where) {
              $where = $where." AND $table\.$key!=".$this->quote($str_val);
            }
            else {
              $where = "WHERE $table\.$key!=".$this->quote($str_val);
            }
          }
          elsif ($str_val =~ /^>/) {
            $str_val =~ s/\s*>\s*//;
            if ($where) {
              $where = $where." AND $table\.$key>".$this->quote($str_val);
            }
            else {
              $where = "WHERE $table\.$key>".$this->quote($str_val);
            }
          }
          elsif ($str_val =~ /^</) {
            $str_val =~ s/\s*<\s*//;
            if ($where) {
              $where = $where." AND $table\.$key<".$this->quote($str_val);
            }
            else {
              $where = "WHERE $table\.$key<".$this->quote($str_val);
            }
          }
          else {
          # Equal:
            if ($where) {
              $where = $where." AND $table\.$key=".$this->quote($str_val);
            }
            else {
              $where = "WHERE $table\.$key=".$this->quote($str_val);
            }
          }
        } 
      }
    }
  
    # Add completly specified where arguments:
    if (@where_statements) {
      foreach my $clause (@where_statements) {
        if ($where) {
          $where = $where." AND ".$clause;
        }
        else {
          $where = "WHERE ".$clause;
        }
      }
    }

   # Add join argument:
   if ($Args->{'join'}) {
     while ((my $key, my $val) = each %{$Args->{'join'}}) {
       if ($where) {
         $where .= " AND $table\.$key=$val";
       }
       else {
         $where = "WHERE $table\.$key=$val";
       }
     }
   }

  }
  
  # The rest of the sql...
  if ($where) {
    $dbq = join(' ',$select,$from,$where);
  }
  else { 
    $dbq = join(' ',$select,$from);
  }

  #########################################################
  #
  # Execute the SQL and get result data set.
  #
  #########################################################

  my $sth = $this->prepare($dbq);

  return {
    'statement_handle' => $sth,
    'bind_list' => \@bind_keys
         };

}


################################################################################
# SUBROUTINE: getColumnNames
#
################################################################################
sub getColumnNames {
  my $this = shift;
  my $table = shift;

  die("ERROR\n QCF::Util::getColumnNames: Must provide a valid database table") if (! $table);  

  # Get all column names for our table:
  my $dbq = "SELECT * FROM $table WHERE 0=1";
  my $sth = $this->prepare($dbq);
  $sth->execute;
  my $cols = $sth->{NAME_lc_hash};
  $sth->finish;

  return $cols;

}


#######################################################################
#  loadTable#  
#  INPUT:
#    Single hashref of arguments:
#      'source_table' - Name of source table
#      'target_table' - Name of target table
#
#  DESCRIPTION:
#    Load all rows from source_table into target_table
#    This function is written with exception 
#    handling that depends on Exception::Class::DBI.
#
#######################################################################
sub loadTable {
  my $this = shift;
  my $Args = shift;

  my $sourceTable = $Args->{'source_table'} if ($Args->{'source_table'});
  my $targetTable = $Args->{'target_table'} if ($Args->{'target_table'});
  my $col_hash = $this->getColumnNames($targetTable);
  my $columns = join(",",keys %$col_hash); 

  croak("Must provide a source table name.\n") if !($sourceTable);
  croak("Must provide a target table name.\n") if !($targetTable);
  print "loading all rows from $sourceTable into $targetTable.\n";

  my $sql= "INSERT /*+APPEND */ INTO $targetTable ($columns) SELECT $columns FROM $sourceTable";

  eval {
  my $sth = $this->do($sql);
  $sth->finish();
  };

  if (my $e =  Exception::Class::DBI->caught()) {
    print STDERR "\nDBI Exception:\n";
    print STDERR "  Exception Type: ", ref $e, "\n";
    print STDERR "  Err:            ", $e->err, "\n";
    $e->rethrow;
  }
}


################################################################################
# This doesn't work yet.....
################################################################################
sub getPrimaryKey {
  my $this = shift;
  my $table = shift;

  die("ERROR\nDB::Util::getPromaryKey: Must provide a valid database table") if (! $table);
  my @cols = $this->primary_key(undef,undef,$table);

  return \@cols;

}

package QCF::Util::st;
our @ISA = qw(QCF::Connection::st);


1;

