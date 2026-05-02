package tn.utm.kafka;

public class Vente {
    private String idClient;
    private double montant;
    private String ville;

    // constructeur vide (obligatoire pour Jackson)
    public Vente() {}

    public Vente(String idClient, double montant, String ville) {
        this.idClient = idClient;
        this.montant = montant;
        this.ville = ville;
    }

    // getters
    public String getIdClient() { return idClient; }
    public double getMontant() { return montant; }
    public String getVille() { return ville; }

    // setters
    public void setIdClient(String idClient) { this.idClient = idClient; }
    public void setMontant(double montant) { this.montant = montant; }
    public void setVille(String ville) { this.ville = ville; }
}
