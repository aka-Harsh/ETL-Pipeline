package com.flowforge.repository;

import com.flowforge.model.NodeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface NodeTypeRepository extends JpaRepository<NodeType, String> {
    List<NodeType> findByCategoryOrderById(String category);
}
