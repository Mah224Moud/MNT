#!/usr/bin/perl
use strict;
use warnings;
use DBI;
use Text::CSV;
use JSON;

# --- 1. Connexion à MariaDB ---
my $dbh = DBI->connect(
    "DBI:MariaDB:database=mnt;host=localhost",
    "",  # Remplace par ton utilisateur MariaDB
    "",  # Remplace par ton mot de passe
    { RaiseError => 1, AutoCommit => 0 }
) or die "Connexion échouée : $DBI::errstr";

# --- 2. Fonction pour insérer un flux ---
sub inserer_flux {
    my ($date, $pays, $cas, $deces, $guerisons, $tests, $source) = @_;
    my $sth = $dbh->prepare(
        "INSERT INTO flux_infini (date_flux, pays, cas, deces, guerisons, tests, source_fichier)
         VALUES (?, ?, ?, ?, ?, ?, ?)"
    );
    $sth->execute($date, $pays, $cas, $deces, $guerisons, $tests, $source);
    $dbh->commit;
    print "Flux inséré : $pays ($date)\n";
}

# --- 3. Parsing du CSV ---
sub parser_csv {
    my $csv = Text::CSV->new({ binary => 1, auto_diag => 2 });
    open my $fh, "<:encoding(utf8)", "donnees_covid.csv" or die "Erreur : $!";
    my $header = $csv->getline($fh);  # Ignore l'en-tête
    while (my $row = $csv->getline($fh)) {
        my ($date, $pays, $cas, $deces, $guerisons, $tests) = @$row;
        inserer_flux($date, $pays, $cas, $deces, $guerisons, $tests, "CSV");
    }
    close $fh;
}

# --- 4. Parsing du JSON ---
sub parser_json {
    open my $fh, "<:encoding(utf8)", "donnees_covid.json" or die "Erreur : $!";
    my $json = do { local $/; <$fh> };
    my $data = decode_json($json);
    for my $row (@$data) {
        inserer_flux(@$row{qw(date pays cas deces guerisons tests)});
    }
    close $fh;
}

# --- 5. Parsing du TXT (TSV) ---
sub parser_tsv {
    open my $fh, "<:encoding(utf8)", "donnees_covid.txt" or die "Erreur : $!";
    while (my $line = <$fh>) {
        chomp $line;
        my ($date, $pays, $cas, $deces, $guerisons, $tests) = split(/\t/, $line);
        inserer_flux($date, $pays, $cas, $deces, $guerisons, $tests, "TSV");
    }
    close $fh;
}

# --- 6. Exécution ---
print "Début de l'import...\n";
parser_csv();
parser_json();
parser_tsv();
print "Import terminé !\n";
$dbh->disconnect;