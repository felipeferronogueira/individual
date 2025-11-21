package school.sptech;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// Ponto de entrada (Main) da AWS Lambda
public class ProcessadorAgregacao implements RequestHandler<S3Event, String> {

    // Cliente S3 Simplificado (usa a IAM Role da Lambda)
    private final S3Client clienteS3 = S3Client.builder().build();

    // Objeto para lidar com JSON
    private final ObjectMapper criadorJson = new ObjectMapper();

    // Serviço que contém a lógica de cálculo
    private final ServicoAgregacao servico = new ServicoAgregacao();

    // Nome do bucket onde o JSON será salvo
    private final String BUCKET_DESTINO = "client-felipeferro";

    @Override
    public String handleRequest(S3Event evento, Context contexto) {
        contexto.getLogger().log("Iniciando agregação modularizada...");

        try {
            // 1. Obter informações do arquivo de origem (trusted)
            String nomeBucketOrigem = evento.getRecords().get(0).getS3().getBucket().getName();
            String chaveArquivo = evento.getRecords().get(0).getS3().getObject().getKey().replace('+', ' ');

            if (!chaveArquivo.endsWith(".csv")) {
                contexto.getLogger().log("Arquivo não é CSV. Ignorando.");
                return "Ignorado";
            }

            // 2. Baixar o arquivo (Stream)
            InputStream fluxoDadosS3 = clienteS3.getObject(GetObjectRequest.builder()
                    .bucket(nomeBucketOrigem)
                    .key(chaveArquivo)
                    .build());

            // 3. Chamar o Serviço para processar e calcular médias
            Map<String, AcumuladorMensal> dadosAgregados = servico.processarCsvParaAgregacao(fluxoDadosS3, contexto);

            // 4. Montar a lista final de médias para JSON
            List<MediaMensal> listaMedias = new ArrayList<>();
            DecimalFormat formatadorDecimal = new DecimalFormat("#.##");

            for (Map.Entry<String, AcumuladorMensal> entrada : dadosAgregados.entrySet()) {
                String mes = entrada.getKey();
                double media = entrada.getValue().obterMedia();

                // Formata e garante o ponto decimal para JSON
                double mediaFormatada = Double.parseDouble(formatadorDecimal.format(media).replace(',', '.'));

                listaMedias.add(new MediaMensal(mes, mediaFormatada));
            }

            // 5. Converter a lista Java para String JSON
            String saidaJson = criadorJson.writeValueAsString(listaMedias);

            // 6. Upload do JSON para o bucket de destino
            String chaveJson = chaveArquivo.replace(".csv", ".json");
            contexto.getLogger().log("Enviando JSON para: " + BUCKET_DESTINO + "/" + chaveJson);

            clienteS3.putObject(PutObjectRequest.builder()
                            .bucket(BUCKET_DESTINO)
                            .key(chaveJson)
                            .contentType("application/json")
                            .build(),
                    RequestBody.fromString(saidaJson));

            return "Sucesso! Agregação concluída.";

        } catch (Exception erro) {
            contexto.getLogger().log("ERRO FATAL: " + erro.getMessage());
            throw new RuntimeException(erro);
        }
    }
}