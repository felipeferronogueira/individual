package school.sptech;

// Classe que define o formato do JSON final
public class MediaMensal {
    // Usamos o padrão camelCase para as variáveis,
    // mas o Jackson converte para o padrão do JSON final
    private final String mes;
    private final double umidadeMedia;

    public MediaMensal(String mes, double umidadeMedia) {
        this.mes = mes;
        this.umidadeMedia = umidadeMedia;
    }

    // Getters obrigatórios para o Jackson
    public String getMes() { return mes; }
    public double getUmidadeMedia() { return umidadeMedia; }
}