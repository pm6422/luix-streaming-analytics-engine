package com.luixtech.frauddetection.simulator.repository;

import com.luixtech.frauddetection.simulator.domain.Detector;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface DetectorRepository extends MongoRepository<Detector, String> {
    List<Detector> findAllByEnabledIsTrue();
}
