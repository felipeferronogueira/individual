package school.sptech;

// Classe simples para somar e contar a umidade de cada mês
public class AcumuladorMensal {
    private double somaUmidade = 0;
    private int contador = 0;

    public void adicionar(int umidade) {
        this.somaUmidade += umidade;
        this.contador++;
    }

    public double obterMedia() {
        if (contador > 0) {
            return somaUmidade / contador;
        } else {
            return 0.0;
        }
    }
}