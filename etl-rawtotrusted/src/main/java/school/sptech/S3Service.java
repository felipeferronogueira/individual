package school.sptech;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import com.amazonaws.services.lambda.runtime.Context;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class S3Service {

    private final S3Client s3 = S3Client.builder()
            .region(Region.US_EAST_1)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();

    // Le todos os CSVs de um prefixo e retorna o conteúdo por estado
    public Map<String, List<String>> listarCsvs(String bucket, String prefix, Context context) {
        Map<String, List<String>> dados = new HashMap<>();

        ListObjectsV2Request req = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix + "/")
                .build();

        ListObjectsV2Response resp = s3.listObjectsV2(req);

        for (S3Object obj : resp.contents()) {
            String key = obj.key();
            if (!key.endsWith(".csv")) continue;

            String estado = key.substring(key.lastIndexOf("_") + 1, key.lastIndexOf(".")).toUpperCase();
            context.getLogger().log("Lendo arquivo: " + key + "\n");

            try (ResponseInputStream<GetObjectResponse> input = s3.getObject(
                    GetObjectRequest.builder().bucket(bucket).key(key).build());
                 BufferedReader reader = new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {

                reader.readLine();
                String linha;
                while ((linha = reader.readLine()) != null) {
                    dados.computeIfAbsent(estado, k -> new ArrayList<>()).add(linha);
                }
            } catch (Exception e) {
                context.getLogger().log("Erro lendo " + key + ": " + e.getMessage() + "\n");
            }
        }

        return dados;
    }

    // Envia um arquivo CSV ao bucket TRUSTED
    public void enviarArquivo(String bucket, String key, String conteudo) {
        s3.putObject(
                PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .contentType("text/csv")
                        .build(),
                RequestBody.fromString(conteudo)
        );
    }
}
