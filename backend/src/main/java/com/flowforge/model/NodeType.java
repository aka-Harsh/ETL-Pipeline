package com.flowforge.model;

import jakarta.persistence.*;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

@Entity
@Table(name = "node_types")
@Data
@NoArgsConstructor
public class NodeType {

    @Id
    private String id;

    @Column(nullable = false, length = 20)
    private String category;

    @Column(name = "display_name", nullable = false, length = 100)
    private String displayName;

    @Column(columnDefinition = "TEXT")
    private String description;

    @Column(name = "config_schema", nullable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private String configSchema;

    @Column(length = 50)
    private String icon;
}
