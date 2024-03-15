package com.luixtech.frauddetection.simulator.repository;

import com.luixtech.frauddetection.simulator.domain.DetectorRule;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DetectorRuleRepository extends MongoRepository<DetectorRule, String> {
}
