package school.sptech;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.InputStream;

// Ponto de entrada (Main) da AWS Lambda (Raw -> Trusted)
public class ProcessadorDadosIniciais implements RequestHandler<S3Event, String> {

    // Cliente S3 Simplificado (usa a IAM Role da Lambda)
    private final S3Client clienteS3 = S3Client.builder().build();

    // Serviço que contém a lógica de transformação
    private final ServicoTransformacao servico = new ServicoTransformacao();

    // Nome do bucket onde o CSV formatado será salvo
    private final String BUCKET_DESTINO = "trusted-felipeferro";

    @Override
    public String handleRequest(S3Event evento, Context contexto) {
        contexto.getLogger().log("Iniciando processamento RAW -> TRUSTED modularizado...");

        try {
            // 1. Obter informações do arquivo de origem (raw)
            String nomeBucketOrigem = evento.getRecords().get(0).getS3().getBucket().getName();
            String chaveArquivo = evento.getRecords().get(0).getS3().getObject().getKey().replace('+', ' ');

            if (!chaveArquivo.endsWith(".csv")) {
                contexto.getLogger().log("Arquivo não é CSV. Ignorando.");
                return "Ignorado";
            }

            contexto.getLogger().log("Baixando arquivo: " + chaveArquivo);

            // 2. Baixar o arquivo (Stream)
            InputStream fluxoDadosS3 = clienteS3.getObject(GetObjectRequest.builder()
                    .bucket(nomeBucketOrigem)
                    .key(chaveArquivo)
                    .build());

            // 3. Define um caminho temporário na memória do Lambda (/tmp/)
            String caminhoArquivoTemporario = "/tmp/formatado_" + chaveArquivo.substring(chaveArquivo.lastIndexOf('/') + 1);

            // 4. CHAMA O SERVIÇO: Processa o InputStream e salva no caminho temporário
            servico.formatarECriarCsv(fluxoDadosS3, caminhoArquivoTemporario, contexto);

            // 5. Upload para o bucket Trusted
            contexto.getLogger().log("Enviando arquivo processado para: " + BUCKET_DESTINO);

            File arquivoTemporario = new File(caminhoArquivoTemporario);

            clienteS3.putObject(PutObjectRequest.builder()
                            .bucket(BUCKET_DESTINO)
                            .key(chaveArquivo) // Mantém a mesma chave/caminho no Trusted
                            .build(),
                    RequestBody.fromFile(arquivoTemporario));

            // 6. Limpar o arquivo temporário
            arquivoTemporario.delete();

            return "Sucesso! CSV formatado e enviado para Trusted.";

        } catch (Exception erro) {
            contexto.getLogger().log("ERRO FATAL: " + erro.getMessage());
            throw new RuntimeException(erro);
        }
    }
}