import io.redisearch.Document;
import io.redisearch.Query;
import io.redisearch.SearchResult;
import io.redisearch.client.Client;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

public class Distributer {
    private Client redisearchClient;
    private ConcurrentMap<String, ClientState> clients;
    private BiConsumer<String, String> updateConsumer;

    public Distributer(Client redisearchClient, ConcurrentMap<String, ClientState> clients,
                       BiConsumer<String, String> updateConsumer) {
        this.redisearchClient = redisearchClient;
        this.clients = clients;
        this.updateConsumer = updateConsumer;
    }

    public void distribute() {
        for (Map.Entry<String, ClientState> entry : clients.entrySet()) {
            List<Document> documents = queryDocuments(entry.getValue());
            List<Document> filteredDocuments = filterDocuments(documents, entry.getValue().getPreviouslySentUpdates());
            sendDocuments(entry.getKey(), entry.getValue(), filteredDocuments);
        }
    }

    private List<Document> queryDocuments(ClientState clientState) {
        String queryString = "@location:[" + clientState.getCenterLongitude()
                + " " + clientState.getCenterLatitude()
                + " " + clientState.getQueryRadius() + " km]";
        System.out.println(queryString);
        Query query = new Query(queryString).setWithPaload();
        SearchResult res = redisearchClient.search(query);
        return res.docs;
    }

    private List<Document> filterDocuments(List<Document> documents, Map<String, Instant> previouslySentUpdates) {
        // TODO notify when entity is deleted or leaves area
        List<Document> filteredDocuments = new ArrayList<>(documents.size());
        for (Document document : documents) {
            Instant updateTime = Instant.ofEpochMilli((Long) document.get("lastUpdateTime"));
            Instant previousUpdateTime = previouslySentUpdates.get(document.getId());
            if (previousUpdateTime == null || updateTime.isAfter(previousUpdateTime)) {
                previouslySentUpdates.put(document.getId(), updateTime);
                filteredDocuments.add(document);
            } else {
                System.out.println("not sending entity with id " + document.getId() + " because it wasn't updated");
            }
        }
        return filteredDocuments;
    }

    private void sendDocuments(String clientName, ClientState clientState, List<Document> documents) {
        for (Document document : documents) {
            String locationString = (String) document.get("location");
            String[] locationParts = locationString.split(",");
            double longitude = Double.parseDouble(locationParts[0]);
            double latitude = Double.parseDouble(locationParts[1]);

            if (clientState.isInBounds(longitude, latitude)) {
                updateConsumer.accept(clientName, new String(document.getPayload(), StandardCharsets.UTF_8));
            }
        }
    }
}
