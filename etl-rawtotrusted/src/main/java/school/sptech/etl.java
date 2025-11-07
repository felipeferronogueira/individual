package school.sptech;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import java.util.Map;

public class etl implements RequestHandler<Object, String> {

    @Override
    public String handleRequest(Object input, Context context) {
        context.getLogger().log("Iniciando ETL de umidade...\n");

        try {
            S3Service s3 = new S3Service();
            UmidadeProcessor processor = new UmidadeProcessor();

            // Baixar e ler CSVs do bucket RAW
            context.getLogger().log("Lendo arquivos do bucket RAW...\n");
            Map<String, Map<String, Double>> medias =
                    processor.calcularMediasMensais(s3.listarCsvs("raw-felipeferro", "Crawler/Dados-Umidade", context));

            // Gerar CSV final
            String csvFinal = processor.gerarCsv(medias);

            // Enviar para bucket TRUSTED
            context.getLogger().log("Enviando resultado para o bucket TRUSTED...\n");
            s3.enviarArquivo("trusted-felipeferro", "Trusted/Umidade-Mensal.csv", csvFinal);

            context.getLogger().log("ETL concluído com sucesso!\n");
            return "ETL finalizado com sucesso!";

        } catch (Exception e) {
            context.getLogger().log("Erro no ETL: " + e.getMessage());
            return "Erro no ETL: " + e.getMessage();
        }
    }
}