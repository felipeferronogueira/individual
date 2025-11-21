package school.sptech;

import com.amazonaws.services.lambda.runtime.Context;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

// Esta classe contém a lógica para ler e calcular
public class ServicoAgregacao {

    // Formato de data de ENTRADA: 2025/01/01 00:00:00
    private static final DateTimeFormatter FORMATADOR_DATA_ENTRADA =  DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

    // Formato da Chave de AGREGAÇÃO: 2025/01
    private static final DateTimeFormatter FORMATADOR_MES = DateTimeFormatter.ofPattern("yyyy/MM");

    public Map<String, AcumuladorMensal> processarCsvParaAgregacao(
            InputStream entrada, Context contexto) throws IOException {

        Map<String, AcumuladorMensal> dadosPorMes = new HashMap<>();

        try (BufferedReader leitor = new BufferedReader(new InputStreamReader(entrada, StandardCharsets.UTF_8))) {
            leitor.readLine(); // Pula o cabeçalho ("horario,umidade")
            String linha;

            while ((linha = leitor.readLine()) != null) {
                if (linha.trim().isEmpty()) continue;

                String[] partes = linha.split(",");

                if (partes.length >= 2) {
                    try {
                        // 1. Transforma a data para obter o mês (a chave de agrupamento)
                        LocalDateTime dataHora = LocalDateTime.parse(partes[0], FORMATADOR_DATA_ENTRADA);
                        String chaveMes = dataHora.format(FORMATADOR_MES);

                        // 2. Converte a umidade para número
                        int umidade = Integer.parseInt(partes[1].trim());

                        // 3. Adiciona ao mapa
                        dadosPorMes.computeIfAbsent(chaveMes, k -> new AcumuladorMensal()).adicionar(umidade);

                    } catch (Exception e) {
                        // Não para o programa se houver erro em apenas uma linha

                        contexto.getLogger().log("Erro ao processar linha: " + linha + " -> " + e.getMessage());
                    }
                }
            }
        }
        return dadosPorMes;
    }
}