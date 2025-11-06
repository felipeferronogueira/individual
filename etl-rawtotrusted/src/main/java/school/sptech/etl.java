package school.sptech;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class etl implements RequestHandler<Object, String> {

    private static final String BUCKET_RAW = "raw-felipeferro";
    private static final String PREFIX = "Crawler/Dados-Umidade";
    private static final String BUCKET_TRUSTED = "trusted-felipeferro";

    private final S3Client s3 = S3Client.builder()
            .region(Region.US_EAST_1)
            .credentialsProvider(DefaultCredentialsProvider.create())
            .build();

    @Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Iniciando ETL de umidade...\n");

        try {
            Map<String, Map<String, List<Double>>> dadosPorEstado = new HashMap<>();

            ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                    .bucket(BUCKET_RAW)
                    .prefix(PREFIX + "/")
                    .build();

            ListObjectsV2Response listResp = s3.listObjectsV2(listReq);
            for (S3Object obj : listResp.contents()) {
                String key = obj.key();
                if (!key.endsWith(".csv")) continue;

                String estado = key.substring(key.lastIndexOf("_") + 1, key.lastIndexOf(".")).toUpperCase();

                context.getLogger().log("Lendo: " + key + "\n");
                ResponseInputStream<GetObjectResponse> s3Input =
                        s3.getObject(GetObjectRequest.builder().bucket(BUCKET_RAW).key(key).build());

                BufferedReader reader = new BufferedReader(new InputStreamReader(s3Input, StandardCharsets.UTF_8));
                reader.readLine();
                String line;

                while ((line = reader.readLine()) != null) {
                    String[] partes = line.split(",");
                    if (partes.length < 2) continue;

                    String timestamp = partes[0];
                    double umidade = Double.parseDouble(partes[1]);

                    LocalDateTime data = LocalDateTime.parse(timestamp, DateTimeFormatter.ISO_DATE_TIME);
                    String mesChave = String.format("%d-%02d", data.getYear(), data.getMonthValue());

                    dadosPorEstado
                            .computeIfAbsent(estado, k -> new HashMap<>())
                            .computeIfAbsent(mesChave, k -> new ArrayList<>())
                            .add(umidade);
                }
            }

            StringBuilder csvSaida = new StringBuilder("estado,ano,mes,media_umidade\n");
            for (var estado : dadosPorEstado.keySet()) {
                for (var mes : dadosPorEstado.get(estado).keySet()) {
                    List<Double> valores = dadosPorEstado.get(estado).get(mes);
                    double media = valores.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);

                    String[] partes = mes.split("-");
                    csvSaida.append(String.format("%s,%s,%s,%.2f\n", estado, partes[0], partes[1], media));
                }
            }

            byte[] bytes = csvSaida.toString().getBytes(StandardCharsets.UTF_8);
            s3.putObject(PutObjectRequest.builder()
                            .bucket(BUCKET_TRUSTED)
                            .key("Trusted/Umidade-Mensal.csv")
                            .contentType("text/csv")
                            .build(),
                    RequestBody.fromBytes(bytes));

            context.getLogger().log("ETL concluído com sucesso!\n");
            return "ETL finalizado: Trusted/Umidade-Mensal.csv";

        } catch (Exception e) {
            context.getLogger().log("Erro no ETL: " + e.getMessage());
            e.printStackTrace();
            return "Erro no ETL: " + e.getMessage();
        }
    }
}
