package com.luixtech.frauddetection.simulator.repository;

import com.luixtech.frauddetection.simulator.domain.RulePayload;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RuleRepository extends JpaRepository<RulePayload, Integer> {
}
