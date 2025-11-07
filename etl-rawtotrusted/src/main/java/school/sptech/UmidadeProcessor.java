package school.sptech;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class UmidadeProcessor {

    // Calcula media mensal de cada estado
    public Map<String, Map<String, Double>> calcularMediasMensais(Map<String, List<String>> dados) {
        Map<String, Map<String, List<Double>>> agrupado = new HashMap<>();

        for (var entrada : dados.entrySet()) {
            String estado = entrada.getKey();
            for (String linha : entrada.getValue()) {
                String[] partes = linha.split(",");
                if (partes.length < 2) continue;

                try {
                    LocalDateTime data = LocalDateTime.parse(partes[0], DateTimeFormatter.ISO_DATE_TIME);
                    double umidade = Double.parseDouble(partes[1]);
                    String mes = String.format("%d-%02d", data.getYear(), data.getMonthValue());

                    agrupado
                            .computeIfAbsent(estado, k -> new HashMap<>())
                            .computeIfAbsent(mes, k -> new ArrayList<>())
                            .add(umidade);

                } catch (Exception ignored) {}
            }
        }

        Map<String, Map<String, Double>> medias = new HashMap<>();
        for (var estado : agrupado.keySet()) {
            Map<String, Double> mediasPorMes = new HashMap<>();
            for (var mes : agrupado.get(estado).keySet()) {
                List<Double> valores = agrupado.get(estado).get(mes);
                double media = valores.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
                mediasPorMes.put(mes, media);
            }
            medias.put(estado, mediasPorMes);
        }

        return medias;
    }

    // Gera o CSV final
    public String gerarCsv(Map<String, Map<String, Double>> medias) {
        StringBuilder sb = new StringBuilder("estado,ano,mes,media_umidade\n");
        for (var estado : medias.keySet()) {
            for (var mes : medias.get(estado).keySet()) {
                String[] partes = mes.split("-");
                sb.append(String.format("%s,%s,%s,%.2f\n",
                        estado, partes[0], partes[1], medias.get(estado).get(mes)));
            }
        }
        return sb.toString();
    }
}
