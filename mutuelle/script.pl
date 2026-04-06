#!/usr/bin/perl
use strict;
use warnings;
use DBI;
use Text::CSV;

# --- 1. Connexion à MariaDB ---
my $dbh = DBI->connect(
    "DBI:MariaDB:database=mnt;host=localhost",
    "",      
    "", 
    { RaiseError => 1, AutoCommit => 0 }
) or die "Connexion échouée : $DBI::errstr";

# --- 2. Fonction pour insérer un flux ---
sub inserer_flux {
    my ($id_flux, $date_reception, $type_document, $id_adherent, $id_contrat, $collectivite,
        $statut_flux, $statut_ged, $date_production, $date_depot_ged, $mode_envoi, $batch_id, $duree_traitement_sec) = @_;

    # Correction automatique des incohérences
    if ($statut_flux eq 'ERREUR_FORMAT' || $statut_flux eq 'ERREUR_DONNEES') {
        $statut_ged = 'NON_APPLICABLE';
        $date_production = undef;
        $date_depot_ged = undef;
    } elsif ($statut_flux eq 'TRAITE') {
        $statut_ged = 'DEPOSE' unless $statut_ged;  # Par défaut, DEPOSE si non défini
    } elsif ($statut_flux =~ /RECU|EN_TRAITEMENT|EN_ATTENTE_DONNEES/) {
        $statut_ged = undef;
    }

    my $sth = $dbh->prepare(
        "INSERT INTO flux_mutuelle (
            id_flux, date_reception, type_document, id_adherent, id_contrat, collectivite,
            statut_flux, statut_ged, date_production, date_depot_ged, mode_envoi, batch_id, duree_traitement_sec
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    );

    $sth->execute(
        $id_flux, $date_reception, $type_document, $id_adherent, $id_contrat, $collectivite,
        $statut_flux, $statut_ged, $date_production, $date_depot_ged, $mode_envoi, $batch_id, $duree_traitement_sec
    );

    $dbh->commit;
    print "Flux inséré : $id_flux ($type_document)\n";
}

# --- 3. Parsing du CSV ---
sub parser_csv {
    my $csv = Text::CSV->new({ binary => 1, auto_diag => 2 });
    open my $fh, "<:encoding(utf8)", "flux_mutuelle.csv" or die "Erreur : $!";

    # Sauter l'en-tête
    my $header = $csv->getline($fh);

    while (my $row = $csv->getline($fh)) {
        my (
            $id_flux, $date_reception, $type_document, $id_adherent, $id_contrat, $collectivite,
            $statut_flux, $statut_ged, $date_production, $date_depot_ged, $mode_envoi, $batch_id, $duree_traitement_sec
        ) = @$row;

        # Nettoyage des champs vides
        $date_production = undef if !$date_production;
        $date_depot_ged = undef if !$date_depot_ged;
        $duree_traitement_sec = undef if !$duree_traitement_sec;

        inserer_flux(
            $id_flux, $date_reception, $type_document, $id_adherent, $id_contrat, $collectivite,
            $statut_flux, $statut_ged, $date_production, $date_depot_ged, $mode_envoi, $batch_id, $duree_traitement_sec
        );
    }
    close $fh;
}

# --- 4. Exécution ---
print "Début de l'import...\n";
parser_csv();
print "Import terminé !\n";