package RegexContainer;

use 5.008008;
use strict;
use warnings;
use Data::Dumper;
use coreutils::DESUtil;
require Exporter;

our @ISA = qw(Exporter);

# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.

# This allows declaration	use RegexContainer ':all';
# If you do not need this, moving things directly into @EXPORT or @EXPORT_OK
# will save memory.
our %EXPORT_TAGS = ( 'all' => [ qw(
	
) ] );

our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );

our @EXPORT = qw(
	
);

our $VERSION = '0.01';

our $debug;

our $verbose;
$verbose = 0;
# Preloaded methods go here.


sub new{

	#my $self  = shift;;
	my ($class,$infoHashref) = @_;
	my $self;
	my $execDefsDetails;
	if (defined $infoHashref->{'verbose'}){
	$verbose = $infoHashref->{'verbose'};
	}
	$self->{_desdbh} = coreutils::DESUtil->new(	DBIattr => {   AutoCommit => 0,     RaiseError => 1,   PrintError => 0	}) or print "\n #### THE ERROR in connecting to db $! ";
	$self->{_desdbh}->{LongReadLen} = 66000;
        $self->{_desdbh}->{LongTruncOk} = 1;
## use the wrapper_instance_id given in the qcf controller param list to get the details about which exec,module,job were used for this qc log file 
	#$execDefsDetails = getExecDefDets($self->{_desdbh},$infoHashref->{'exec_names'}); 
	$self->{wrapper_instance_id} = $infoHashref->{'wrapper_instance_id'};
	$self->{exec_names} = $infoHashref->{'exec_names'};
	#$self->{exec_id} = $execDefsDetails->{'pfw_executable_id'};
	#$self->{module_id} = $execDefsDetails->{'pfw_module_id'};
	#$self->{desjob_id} = $execDefsDetails->{'pfw_job_id'};
	#$self->{id} = $execDefsDetails->{'id'};
	#$self->{count} = $execDefsDetails->{'count'};
        $self->{regex_hash} = getRegexHash($self, $self->{exec_names});
	# Must be done.
	bless($self, $class);           # but see below
	if($verbose >=2){print "\n the constructor for RegexContainer. It contains regexhash as ",Dumper($self->{regex_hash});}
        return $self;
}

####
### This subroutine helps in resolving a given execdefs table id to its components: exec Table ID, Module ID, DES Job ID, and iteration number for this particular exec call within a module.
####
sub getExecDefDets {
	
	my ($desdbh,$wrapperInstanceId) = @_;
	
	my $sql = "select * from pfw_executable_def where id = $wrapperInstanceId";
	print "\n the sql to get executable information is $sql " if($verbose >= 2);
	my $sth = $desdbh->prepare($sql);
	$sth->execute();
	return $sth->fetchrow_hashref();
}


sub getRegexHash {
        my $self = shift;
	my ($exec_names) = @_;
	my $ret;
	my $patternDBHashRef;
	my $patternHash;
	my $variablesHash;
	my $variablesDBHashRef;
	my ($imageIdHashRef,$tempPattern);
	# Get the qc framework variables first.
	my $variablesSql = qq{select * from QC_VARIABLE  where valid = 'y' order by PATTERN_LOCATION};
	my $variablesSth = $self->{_desdbh}->prepare($variablesSql) or print "Error in preparing $!";
	$variablesSth->execute() or print "\n #### ERROR FETCHING QC PATTERNS: $!";

	# put singe quotes around the names of execs, so that we can query against them 
	$exec_names =~  s/([^,\s]+)\s*(,?)/'$1'$2/g;
	while($variablesDBHashRef = $variablesSth->fetchrow_hashref()){
		push @{$variablesHash->{$variablesDBHashRef->{'pattern_id'}}}, $variablesDBHashRef;

		#### if the action code is set for this variable, perform specific actions on the basis of that.
		#### If the action code is 1, set this variable as the image Id for the rest of the variables, untill a new variable is found.	
		#### to that purpose, setup a hashref indexed on patternid, with the id of the variable whose action_code was set to 1
		if(defined $variablesDBHashRef->{'action_code'} && $variablesDBHashRef->{'action_code'}== 1)
		{
			$imageIdHashRef->{$variablesDBHashRef->{'pattern_id'}} = $variablesDBHashRef->{'id'};	
		}
		
	}

	my $variablesIdHash;	
	my $eachVar;
	#### Now loop through all the variables under each Pattern, and assign the Image Id variable Id to each.
	foreach my $varHashRefPatternId ( keys %$variablesHash){
		foreach $variablesIdHash (@{$variablesHash->{$varHashRefPatternId}}) {
			$variablesIdHash->{'image_id'} = $imageIdHashRef->{$varHashRefPatternId};
			#print "\n ACTIONCODE the separate var ", Dumper($variablesIdHash);
		}
	}
	
	#### Get all the valid patterns for the exec(utable), whose output is being parsed for the QC information. This exec id is linked to the exec table, which contains a list of all the executables in the system
	my $patternSql = qq{select * from QC_PATTERN  where valid = 'y' and execname in ( $exec_names,'global')  };
	print "\n the query to get patterns: $patternSql" if ($verbose >= 2);
	my $patternSth = $self->{_desdbh}->prepare($patternSql) or print "Error in preparing $!";
	$patternSth->execute();

	while($patternDBHashRef = $patternSth->fetchrow_hashref()){

		###
		# insert the hashref containing all the variables associated with the pattern, into the patternhashref. Now we have a patternhashref with all the information about the pattern from qc_pattern and qc_variable tables
		###
		$patternDBHashRef->{'variables'} = $variablesHash->{$patternDBHashRef->{'id'}};
		$tempPattern =  $patternDBHashRef->{'pattern'};
		$patternDBHashRef->{'pattern_compiled'} = qr/$tempPattern/sm;
		push@{$patternHash}, $patternDBHashRef;
	}

	if( $verbose >= 2){ print "\n The Pattern Hash Ref with information about variables embedded in it: \n",Dumper($patternHash);}	
	#print "\n in variables  Hash",Dumper($variablesHash);
	return $patternHash;
}

###
# This function gets the regex hash for execid extracted from the execdefs table , whose wrapper_instance_id was provided in the param list of qcf controller
###
sub getRegexHash_old {
        my $self = shift;
	my ($exec_id) = @_;
	my $ret;
	my $patternDBHashRef;
	my $patternHash;
	my $variablesHash;
	my $variablesDBHashRef;
	my $imageIdHashRef;
	# Get the qc framework variables first.
	my $variablesSql = qq{select * from QC_VARIABLE  where valid = 'y' order by PATTERN_LOCATION};
	my $variablesSth = $self->{_desdbh}->prepare($variablesSql) or print "Error in preparing $!";
	$variablesSth->execute() or print "\n #### ERROR FETCHING QC PATTERNS: $!";

	while($variablesDBHashRef = $variablesSth->fetchrow_hashref()){

	push @{$variablesHash->{$variablesDBHashRef->{'pattern_id'}}}, $variablesDBHashRef;

	#### if the action code is set for this variable, perform specific actions on the basis of that.
	#### If the action code is 1, set this variable as the image Id for the rest of the variables, untill a new variable is found.	
	#### to that purpose, setup a hashref indexed on patternid, with the id of the variable whose action_code was set to 1
	if(defined $variablesDBHashRef->{'action_code'} && $variablesDBHashRef->{'action_code'}== 1)
	{
		$imageIdHashRef->{$variablesDBHashRef->{'pattern_id'}} = $variablesDBHashRef->{'id'};	
	}
	
	}

	my $variablesIdHash;	
	my $eachVar;
	#### Now loop through all the variables under each Pattern, and assign the Image Id variable Id to each.
	foreach my $varHashRefPatternId ( keys %$variablesHash){
		foreach $variablesIdHash (@{$variablesHash->{$varHashRefPatternId}}) {
			$variablesIdHash->{'image_id'} = $imageIdHashRef->{$varHashRefPatternId};
			#print "\n ACTIONCODE the separate var ", Dumper($variablesIdHash);
		}
	}
	
	#### Get all the valid patterns for the exec(utable), whose output is being parsed for the QC information. This exec id is linked to the exec table, which contains a list of all the executables in the system
	my $patternSql = qq{select * from QC_PATTERN  where valid = 'y' and pfw_executable_id in( $exec_id,0)  };
	print "\n the query to get patterns: $patternSql" if ($verbose >= 2);
	my $patternSth = $self->{_desdbh}->prepare($patternSql) or print "Error in preparing $!";
	$patternSth->execute();

	while($patternDBHashRef = $patternSth->fetchrow_hashref()){

		###
		# insert the hashref containing all the variables associated with the pattern, into the patternhashref. Now we have a patternhashref with all the information about the pattern from qc_pattern and qc_variable tables
		###
		$patternDBHashRef->{'variables'} = $variablesHash->{$patternDBHashRef->{'id'}};
		push@{$patternHash}, $patternDBHashRef;
	}

	if( $verbose >= 2){ print "\n The Pattern Hash Ref with information about variables embedded in it: \n",Dumper($patternHash);}	
	#print "\n in variables  Hash",Dumper($variablesHash);
	return $patternHash;
}

sub DESTROY {
	
	my $self = shift;
	$self->{_desdbh}->disconnect();
	print "\n ##### DESTROYING REGEXCONTAINER" if( $verbose >= 3);
}

1;
__END__
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

RegexContainer - Perl extension for blah blah blah

=head1 SYNOPSIS

  use RegexContainer;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for RegexContainer, created by h2xs. It looks like the
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
