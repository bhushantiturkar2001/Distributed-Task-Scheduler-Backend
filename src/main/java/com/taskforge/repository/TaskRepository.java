package com.taskforge.repository;

import com.taskforge.model.Task;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public interface TaskRepository extends JpaRepository<Task, UUID> {

    Page<Task> findByStatus(Task.TaskStatus status, Pageable pageable);

    @Query("SELECT t FROM Task t WHERE t.status = 'PENDING' AND t.scheduledAt <= :now ORDER BY t.priority DESC, t.scheduledAt ASC")
    List<Task> findDueTasks(LocalDateTime now);

    long countByStatus(Task.TaskStatus status);
}
