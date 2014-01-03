package com.amazon.messaging.utils.failureTrackers;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.Lists;


public class WeightedRandomEndpointSelector {
    
    private static Random random = new Random(System.currentTimeMillis());
    
    /**
     * Returns a randomly chosen list of active endpoints with a size up to
     * 'count' based on the provided weight map. The weights of the endpoints should be positive.
     */
    public static List<String> getWeightedRandomEndpoints(Map<String, Double> endpointWeights, int count) {

        List<String> chosenEndpoints = Lists.newArrayList();

        double endpointWeightSum = 0;
        for (Map.Entry<String, Double> endpointWeight : endpointWeights.entrySet()) {
            if (endpointWeight.getValue() >= 0) {
                endpointWeightSum += endpointWeight.getValue();
            } else {
                throw new IllegalArgumentException("Negative weights for endpoints are not supported");
            }
        }

        while (chosenEndpoints.size() < count && endpointWeights.size() > 0) {
            double randomValue = random.nextDouble() * endpointWeightSum;
            for (Iterator<Map.Entry<String, Double>> endPointIterator = endpointWeights.entrySet().iterator(); endPointIterator.hasNext();) {
                Map.Entry<String, Double> currentHostEntry = endPointIterator.next();
                double weight = currentHostEntry.getValue();
                randomValue -= weight;
                if (randomValue <= 0) {
                    chosenEndpoints.add(currentHostEntry.getKey());
                    endpointWeightSum -= weight;
                    endPointIterator.remove();
                    break;
                }
            }
        }

        return chosenEndpoints;
    }
}
