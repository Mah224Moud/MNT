#!/usr/bin/perl
use strict;
use warnings;
use DBI;

my $dbh = DBI->connect(
    "DBI:MariaDB:database=test;host=localhost",
    "root",  # Remplace par ton utilisateur MariaDB
    "",  # Remplace par ton mot de passe
    { RaiseError => 1 }
) or die "Connexion échouée : $DBI::errstr";

print "Connexion réussie à MariaDB !\n";
$dbh->disconnect;