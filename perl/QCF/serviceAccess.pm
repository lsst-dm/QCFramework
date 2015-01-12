#  $Id$
#  $Rev$: Revision of last commit
#  $Author$: Author of last commit
#  $Date$: Date of last commit

package QCF::serviceAccess;

use strict;
use warnings;
use Data::Dumper;
use Config::INI::Reader;
use Fcntl ':mode';
use Carp;

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration	use serviceAccess ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.
our %EXPORT_TAGS = ( 'all' => [ qw(
	
) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(
	
);

our $VERSION = '0.01';


# Preloaded methods go here.

sub getServiceAccessDetails {

	my ($file,$section,$tag,$loose,$format) = @_;
	my ($homePath,$retHash,$eventStr,$dbfile);
	
	# initialize file from env variable DES_SERVICES if it is not available
	unless (defined $file){
		if(defined $ENV{"DES_SERVICES"}){
			$file = $ENV{"DES_SERVICES"};
		}
	}

	# initialize file from env variable from HOME and .desservices.ini if it is not available
	unless (defined $file){
		$file = $ENV{"HOME"}.'/.desservices.ini';
	}

	unless (-r $file){ # if file is readable (and therefore also present), go ahead and read it
		carp "cannot read db config file: $file";
	}

	if (!defined $section  &&  defined $tag){
		$tag = uc($tag); 
		$section = $ENV{"DES_$tag"."_SECTION"};
        unless (defined $section){
        #if($section eq ''){
            croak "Insufficient information to make a DB Connection.\nPlease provide at least one of:\nEnvironment Variable: DES_DB_SECTION\nA valid section parameter from desservices.ini file\n";
        }
	}else{
        #print "\n i have the section $section";
    }

	$retHash = Config::INI::Reader->read_file($file);
	
	$retHash->{'meta_file'} = $file;
	$retHash->{'meta_section'} = $section;	
	if (defined $tag && lc($tag) eq "db"){
		$retHash = process_db($retHash);
	}

	return $retHash;
}

# add more information to the database information sent
# type shall be defaulted to oracle if it is not provided in desservices file
sub process_db {

	my ($dbHash) = @_;
	#print "\n processing DB,",Dumper($dbHash);
	unless (defined $dbHash->{$dbHash->{'meta_section'}}->{'type'}){
		$dbHash->{$dbHash->{'meta_section'}}->{'type'} = 'oracle';
	}
	if(defined $dbHash->{$dbHash->{'meta_section'}}->{'type'} && $dbHash->{$dbHash->{'meta_section'}}->{'type'}  eq ''){
		$dbHash->{$dbHash->{'meta_section'}}->{'type'} = 'oracle';
	}
	unless (defined $dbHash->{$dbHash->{'meta_section'}}->{'sid'}){
		$dbHash->{$dbHash->{'meta_section'}}->{'sid'} = '';
	}
	if(defined $dbHash->{$dbHash->{'meta_section'}}->{'sid'} && $dbHash->{$dbHash->{'meta_section'}}->{'sid'} eq ''){
		$dbHash->{$dbHash->{'meta_section'}}->{'sid'} = '';
	}
	unless (defined $dbHash->{$dbHash->{'meta_section'}}->{'name'}){
		$dbHash->{$dbHash->{'meta_section'}}->{'name'} = '';
	}
	if(defined $dbHash->{$dbHash->{'meta_section'}}->{'name'} && $dbHash->{$dbHash->{'meta_section'}}->{'name'} eq ''){
		$dbHash->{$dbHash->{'meta_section'}}->{'name'} = '';
	}

	if ($dbHash->{$dbHash->{'meta_section'}}->{'type'} =~ /oracle/i){
		$dbHash->{$dbHash->{'meta_section'}}->{'port'} = '1521';
	}
	if ($dbHash->{$dbHash->{'meta_section'}}->{'type'} =~  /postgres/i){
		$dbHash->{$dbHash->{'meta_section'}}->{'port'} = '5432';
	}
	#print "\n processing DB After ,",Dumper($dbHash);
    
    return $dbHash;

}

# check for permissions on the .desservices.ini file, user should have all permissions. group and other have no permissions. see DESDM-3 on the wiki
sub check{

	my ($dbDets) = @_;
	#  "raise exception if file or indicated keys inconsistent with DESDM-3."

	my($permission_faults,$permission_checks,$mode,$user_rwx,$group_read,$group_write,$other_read,$other_write,$error,$filename);
	$filename = $dbDets->{'dbfile'};
	$error = 0;
	$mode = (stat($filename))[2];
	$user_rwx = ($mode & S_IRWXU) >> 6;
	$group_read = ($mode & S_IRGRP) >> 3;
	$group_write = ($mode & S_IWGRP) >> 3;
	$other_read = $mode & S_IROTH;	
	$other_write = $mode & S_IWOTH;	
	if($user_rwx < 4 ){
		print "\n faulty permissions for dbconfig file. Effective user should have atleast read permissions. Permissions are: $user_rwx";
		$error=1;
	}
	if($group_read != 0){
		print "\n faulty permissions for dbconfig file. Group has permissions: $group_read on the file";
		$error=1;
	}
	if($group_write != 0){
		print "\n faulty permissions for dbconfig file. Group has permissions: $group_write on the file";
		$error=1;
	}
	if($other_read != 0){
		print "\n faulty permissions. Others have permissions: $other_read on the file";
		$error=1;
	}
	if($other_write != 0){
		print "\n faulty permissions. Others have permissions: $other_write on the file";
		$error=1;
	}

	QCF::serviceAccess::check_db($dbDets);
	
	return $error;
}

sub check_db {

  my ($dbDets) = @_;
  my (@missing,@extra,$key,$found);
  my @ConnectDataKeys = ('type','server','port','user','passwd');


  foreach my $key (@ConnectDataKeys) {
	unless (defined $dbDets->{$key}){
		push @missing, $key;
	}
  }

  foreach $key (keys %$dbDets){

	$found = grep $key, @ConnectDataKeys;
	unless (defined $found){
		push @extra, $key;
	} 
  }

	if(scalar @extra > 0 || scalar @missing > 0){
	
		print "\n faulty keys :  ",@extra," ",@missing;	
	}

}

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

serviceAccess - Perl extension for creating, verifying and storing checksum values for all files in the DES system.

=head1 SYNOPSIS

  use serviceAccess;
  blah blah blah

=head1 DESCRIPTION


The serviceAccess module has the following functionality:

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

Ankit Chandra, E<lt>ankitc@ncsa.illinois.edu<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2010 by Ankit Chandra

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.8 or,
at your option, any later version of Perl 5 you may have available.


=cut
