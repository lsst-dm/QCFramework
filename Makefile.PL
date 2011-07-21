
use File::Find;
use ExtUtils::MakeMaker;

my %REQUIRED_MODULES = (
        'DBI'            => 1.602,
        'DBD::Oracle'    => 1.20,
        'List::Util' => 1.19,
        'Exception::Class' => 1.23,
        'Config::General' => 2.38,
);

WriteMakefile(
    NAME            => 'DAA',
    EXE_FILES   => [
                     'bin/qcf_controller.pl',
                     'bin/qcf_status.pl',
                     'bin/qcf_createNewPattern.pl',
                   ],
    PMLIBDIRS => ["./lib"],
    PREREQ_PM => \%REQUIRED_MODULES,
    PL_FILES => {},
);
