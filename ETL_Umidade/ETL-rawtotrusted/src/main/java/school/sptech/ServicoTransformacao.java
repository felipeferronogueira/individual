package school.sptech;

import com.amazonaws.services.lambda.runtime.Context;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

// Esta classe contém a lógica para ler o CSV, formatar a data e salvar o novo arquivo.
public class ServicoTransformacao {

    // Formato de ENTRADA: 2025-01-01T00:00 (do crawler Python)
    private static final DateTimeFormatter FORMATADOR_DATA_ENTRADA = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

    // Formato de SAÍDA: 2025/01/01 00:00:00 (para o Trusted)
    private static final DateTimeFormatter FORMATADOR_DATA_SAIDA = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");

    public void formatarECriarCsv(
            InputStream entradaDados,
            String caminhoSaidaTemporario,
            Context contexto
    ) throws IOException {

        try (BufferedReader leitor = new BufferedReader(new InputStreamReader(entradaDados, StandardCharsets.UTF_8));
             BufferedWriter escritor = new BufferedWriter(new FileWriter(caminhoSaidaTemporario))) {

            String linha;
            boolean ehPrimeiraLinha = true;

            while ((linha = leitor.readLine()) != null) {
                if (linha.trim().isEmpty()) continue;

                if (ehPrimeiraLinha) {
                    // Escreve o NOVO cabeçalho padronizado para o Trusted
                    escritor.write("horario,umidade");
                    escritor.newLine();
                    ehPrimeiraLinha = false;
                    continue;
                }

                String[] partes = linha.split(",");

                if (partes.length >= 2) {
                    try {
                        // 1. Converte a data do formato de entrada
                        LocalDateTime dataHora = LocalDateTime.parse(partes[0], FORMATADOR_DATA_ENTRADA);

                        // 2. Formata para o formato de saída
                        String dataFormatada = dataHora.format(FORMATADOR_DATA_SAIDA);

                        // 3. Escreve a linha tratada: DataNova,ValorOriginal
                        escritor.write(dataFormatada + "," + partes[1]);
                        escritor.newLine();

                    } catch (Exception e) {
                        contexto.getLogger().log("Erro ao formatar data/valor na linha: " + linha);
                    }
                }
            }
        }
    }
}