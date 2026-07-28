package com.taskforge.repository;

import com.taskforge.model.ExecutionLog;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.UUID;

@Repository
public interface ExecutionLogRepository extends JpaRepository<ExecutionLog, UUID> {

    Page<ExecutionLog> findByTaskIdOrderByCreatedAtDesc(UUID taskId, Pageable pageable);

    List<ExecutionLog> findByTaskIdOrderByAttemptNumberDesc(UUID taskId);

    long countByStatus(ExecutionLog.ExecutionStatus status);

    @Query("SELECT COUNT(el) FROM ExecutionLog el WHERE el.status = 'SUCCESS'")
    long countSuccessfulExecutions();

    @Query("SELECT AVG(el.durationMs) FROM ExecutionLog el WHERE el.status = 'SUCCESS' AND el.durationMs IS NOT NULL")
    Double getAverageExecutionDuration();

    List<ExecutionLog> findByWorkerIdOrderByCreatedAtDesc(String workerId);
}
