import java.io.*;
import java.net.URI;
import java.net.URL;
import java.nio.file.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.zip.ZipFile;

public class DownloadAndExtract {
    private static final List<String> DOWNLOAD_URIS = Arrays.asList(
            "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
            "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
            "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
            "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
            "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
            "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
            "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip"
    );

    public static void main(String[] args) {
        CompletableFuture[] futures = DOWNLOAD_URIS.stream()
                .map(uri -> CompletableFuture.runAsync(() -> downloadAndExtractFile(uri)))
                .toArray(CompletableFuture[]::new);

        CompletableFuture.allOf(futures).join();
    }

    private static void downloadAndExtractFile(String uri) {
        try {
            URL url = new URL(uri);
            String filename = Paths.get(url.getPath()).getFileName().toString();

            Path downloadDir = Paths.get("downloads");
            if (!Files.exists(downloadDir)) {
                Files.createDirectories(downloadDir);
            }

            try (InputStream inputStream = url.openStream()) {
                Files.copy(inputStream, downloadDir.resolve(filename), StandardCopyOption.REPLACE_EXISTING);
            }

            try (ZipFile zipFile = new ZipFile(downloadDir.resolve(filename).toFile())) {
                zipFile.stream()
                        .filter(zipEntry -> zipEntry.getName().toLowerCase().endsWith(".csv"))
                        .forEach(zipEntry -> {
                            try (InputStream entryInputStream = zipFile.getInputStream(zipEntry)) {
                                String csvFileName = Paths.get(zipEntry.getName()).getFileName().toString();
                                Path csvFilePath = downloadDir.resolve(csvFileName);
                                Files.copy(entryInputStream, csvFilePath, StandardCopyOption.REPLACE_EXISTING);

                                // Update the all_downloaded_data.zip file
                                try (FileSystem zipfs = FileSystems.newFileSystem(
                                        URI.create("jar:file:" +
                                                Paths.get(System.getProperty("user.dir"), "all_downloaded_data.zip")),
                                        null)) {
                                    Path dest = zipfs.getPath(zipEntry.getName());
                                    Files.copy(csvFilePath, dest, StandardCopyOption.REPLACE_EXISTING);
                                }
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        });
            }

            Files.delete(downloadDir.resolve(filename));

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
