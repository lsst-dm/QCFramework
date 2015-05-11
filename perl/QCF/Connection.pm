########################################################################
#
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
################################################################################
################################################################################
package QCF::Connection;
use strict;
use warnings;
use NEXT;
use Data::Dumper;
use Switch;
use Carp;
use Sys::Hostname;
use POSIX qw(strftime);
#use FindBin;
#use lib ("$FindBin::Bin/../lib/perl5","$FindBin::Bin/../lib");
use QCF::serviceAccess;
use base qw(DBI);

################################################################################
# Constructor for a new DB::Connection object which is really a DBI::db object
# blessed into the calling Class::db.
#
# THIS CONSTRUCTOR MUST BE CALLED
#  --It will not work unless leftmost in the @ISA since
#  DBI connect requires that it bless its' own objects.
#  
################################################################################
sub new {
  my ($this, %Args) = @_;
  my $self = $this->connect(%Args);
  $self->EVERY::LAST::_init(%Args);
  return $self;
}


sub connect {

  my ($self, %Args) = @_;

  my ($dbh, %ConnectData, $type, $server, $name, $sid, $user, $pass, $connectTarget, $connectDescriptor,$section,$config_file,$tag,$port,$dbDets);
  my $DBIattr = {};
  my $ConfigVals = {};
  my @ConnectDataKeys = ('type','server','name','user','passwd','port');
  $config_file = $Args{'db_config_file'};
  $section = $Args{'db_config_file_section'};
  $tag = $Args{'db_config_file_tag'};

  # If a config file is said to exist, parse it.
  # $ConfigVals = $self->parse_db_config_file($Args{'db_config_file'}) if ($Args{'db_config_file'});
  $ConfigVals = QCF::serviceAccess::getServiceAccessDetails($config_file,$section,$tag);
  foreach my $configIter (keys %{$ConfigVals->{$ConfigVals->{'meta_section'}}}){
    $ConfigVals->{$ConfigVals->{'meta_section'}}->{lc($configIter)} = $ConfigVals->{$ConfigVals->{'meta_section'}}->{$configIter};
  }
  
    #print "\n the dump ",Dumper($ConfigVals);
  $section = (defined $Args{'db_config_file_section'}? $Args{'db_config_file_section'}:$ConfigVals->{'meta_section'});


  # If a hasf referece is provided as an argument, assume these are DBI attributes 
  # to be passed to the connection:
  foreach my $Arg (values %Args) {
    if (ref $Arg eq 'HASH') {
      $DBIattr = $Arg;
      last;
    }
  }

  # Fill in the gaps or override with stuff from Args:
  foreach my $key (@ConnectDataKeys) {
    if (defined $Args{$key}) {
      $ConnectData{$key} = $Args{$key};
    }
    elsif (defined $ConfigVals->{$section}->{$key}) {
      $ConnectData{$key} = $ConfigVals->{$section}->{lc($key)};
    }
    else {
      croak("Insufficient data to make a database connection.\nMust provide $key.\n");
    }
  }
  $type = $ConnectData{'type'};
  $server = $ConnectData{'server'};
  $name = $ConnectData{'name'};
  $sid = $ConnectData{'db_sid'};
  $user = $ConnectData{'user'};
  $pass = $ConnectData{'passwd'};
  $port = $ConnectData{'port'};
  
  $dbDets->{'dbfile'} = (defined $config_file? $config_file:$ConfigVals->{'meta_file'}); 
  $dbDets->{'type'} = $type; 
  $dbDets->{'server'} = $server; 
  $dbDets->{'user'} = $user; 
  $dbDets->{'passwd'} = $pass; 
  $dbDets->{'port'} = $port; 
  QCF::serviceAccess::check($dbDets);

  ##############################################################################
  # Oracle connection
  ##############################################################################
  if ($type =~ /ORACLE/i) {

	# using SERVICE in place of SID because it is more generic, unless specific db_sid is set
	if(defined $sid && $sid ne '') {
	  $connectTarget = "(SID=$sid)";
	}
	else {
	  $connectTarget = "(SERVICE_NAME=$name)";
	}
	$connectDescriptor = "(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=$server)(PORT=$port))(CONNECT_DATA=(SERVER=dedicated)$connectTarget))";

    my $MAXTRIES = 5;
    my $TRY_DELAY = 10;
    my $trycnt = 0;
    my $done = 0;
    my $lasterr = "";
    while (!$done && ($trycnt < $MAXTRIES)) {
        $trycnt += 1;
        
        eval { $dbh = $self->SUPER::connect("DBI:Oracle:$connectDescriptor",$user,$pass, $DBIattr); };
        if ($@) {
            $lasterr = $DBI::errstr;
            my $timestamp = strftime("%x %H:%M:%S", localtime());
            print "$timestamp: Error when trying to connect to database: $lasterr\n";
            if ($trycnt < $MAXTRIES) {
                print "\tRetrying...\n\n";
                sleep($TRY_DELAY);
            }
        }
        else {
            $done = 1;
        }
    }

    if (!$done) {
        print "Exechost: ", hostname, "\n";
        print "Connection information:\n";
        foreach my $key ("user", "type", "port", "server") {
            print "\t$key = "; 
            if (defined($ConnectData{$key})) {
                print $ConnectData{$key}, "\n";
            }
            else {
                print "UNDEF\n";
            }
        }
        print "\n";
        croak("Aborting attempt to connect to database.  Last error message: $lasterr");
    }
    elsif ($trycnt > 1) { # only print success message if we've printed failure message
        print "Successfully connected to database after retrying.\n";
    }




    # Verbose connection status output:
    if ($Args{'debug'}) {
      print "Successfully connected to the database on $server using $connectTarget.\n";
      if ($DBIattr) {
        print "Connected with the following DBI attributes:\n";
        while ((my $k, my $v) = each %{$DBIattr}) {
          print "$k = $v\n";
        }
      }
      print "\n";
    }
  }
  ##############################################################################
  # Another connection...
  ##############################################################################

  else {
    croak("No connection method yet defined for db_type: $type.\n");
  }

  return $dbh;
}


################################################################################
#
################################################################################
sub parse_db_config_file {
  my $self = shift;
  my $file = shift;
  my %Data;

    croak("Unable to read: $file\nto parse for database connection information.\n") if (! -r $file);

    # Knock permissions back to 0600 to keep passwords safe.
    # TODO - Check current permissions and warn user if not strong enough
    chmod 0600, $file or warn "Unable to chmod $file, maybe you don't own the file...\n";

    open(FH, "< ".$file) or croak("Unable to open: $file\n to parse for database connection information.\n");

    # Parse the config file, looking for the standard info:
    my ($line, $left, $right);
    my $linenum = 0;
    while($line = <FH>) {
      $linenum++;
      $line =~  s/^\s+//g;
      $line =~ s/\s+$//g;
      if (($line =~ /\S/) && ($line !~ /^#/))
      {
         if ($line =~ /=/)
         {
            ($left, $right) = $line =~ m/^(\S+)\s*=\s*(\S.*)$/;
         }
         else
         {
            ($left, $right) = $line =~ m/^(\S+)\s*(.*)$/;
         }
         $left =~ tr/A-Z/a-z/;
         switch ($left)
         {
            case /^db_type$/i   { $Data{'db_type'}   = $right; }
            case /^db_server$/i { $Data{'db_server'} = $right; }
            case /^db_name$/i   { $Data{'db_name'}   = $right; }
            case /^db_service$/i    { $Data{'db_name'}   = $right; }
            case /^db_sid$/i    { $Data{'db_sid'}   = $right; }
            case /^db_user$/i   { $Data{'db_user'}   = $right; }
            case /^db_pass$/i   { $Data{'db_pass'}   = $right; }
            case /^DB_PASSWD$/i { $Data{'db_pass'}   = $right; }
            case /^db_server_standby$/i { $Data{'db_server_standby'}   = $right; }
            case /^db_name_standby$/i { $Data{'db_name_standby'}   = $right; }
            else
            {
               warn "\nWARNING: Unrecognized line:\n$line\nin db config file:'$file'\n";
            }
         }
      }
    }

  close FH;

  return \%Data;

}

package QCF::Connection::db;
our @ISA = qw(DBI::db);

################################################################################
#  Initialize class data for all derived objects of this class.
################################################################################
sub _init {
  my ($self, %Args) = @_;
  #print "DB::Connection::db::_init\n";
  $self->verbose(1);
  $self->debug(0);
  $self->verbose($Args{'verbose'}) if (defined $Args{'verbose'});
  $self->debug($Args{'debug'}) if (defined $Args{'debug'});
  return $self;
}


{
  my %ClassData = ();

  sub verbose {
    my($self, $newvalue) = @_;
    if (@_ > 1) {
      $ClassData{'verbose'} = $newvalue;
    }
    return $ClassData{'verbose'};
  }

  sub debug {
    my($self, $newvalue) = @_;
    if (@_ > 1) {
      $ClassData{'debug'} = $newvalue;
    }
    return $ClassData{'debug'};
  }
}

package QCF::Connection::st;
our @ISA = qw(DBI::st);

1;
