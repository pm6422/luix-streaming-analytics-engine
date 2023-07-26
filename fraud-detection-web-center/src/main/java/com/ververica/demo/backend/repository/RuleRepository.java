package com.ververica.demo.backend.repository;

import com.ververica.demo.backend.entities.Rule;
import org.springframework.data.jpa.repository.JpaRepository;

public interface RuleRepository extends JpaRepository<Rule, Integer> {
}
