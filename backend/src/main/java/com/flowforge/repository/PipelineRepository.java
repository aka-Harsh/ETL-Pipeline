package com.flowforge.repository;

import com.flowforge.model.Pipeline;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface PipelineRepository extends JpaRepository<Pipeline, UUID> {
    List<Pipeline> findByIsActiveTrueOrderByCreatedAtDesc();
}
