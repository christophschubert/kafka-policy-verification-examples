package net.christophschubert.kafka.policy;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EnforceReplicationFactorThree implements CreateTopicPolicy {

    private final static Logger logger = LoggerFactory.getLogger(EnforceReplicationFactorThree.class);

    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        logger.info("Invoked validate with configs {}", requestMetadata);
        if (requestMetadata.replicationFactor() != 3) {
            final var msg = String.format("Replication factor for topic %s should be 3, was %d}",
                    requestMetadata.topic(),
                    requestMetadata.replicationFactor()
            );
            throw new PolicyViolationException(msg);
        }
//        if (requestMetadata.configs().getOrDefault())
    }

    @Override
    public void close() throws Exception {
        //nothing to do!
    }

    @Override
    public void configure(Map<String, ?> configs) {
        logger.info("Invoked configure with configs {}", configs);
    }
}
