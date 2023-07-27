package com.luixtech.frauddetection.simulator.repository;

import com.luixtech.frauddetection.simulator.domain.Rule;
import org.springframework.data.jpa.repository.JpaRepository;
public interface RuleRepository extends JpaRepository<Rule, Integer> {
}
