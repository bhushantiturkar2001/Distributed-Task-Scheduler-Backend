package com.taskforge.repository;

import com.taskforge.model.TaskSchedule;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public interface TaskScheduleRepository extends JpaRepository<TaskSchedule, UUID> {

    @Query("SELECT ts FROM TaskSchedule ts WHERE ts.isActive = true AND ts.nextRunAt <= :now")
    List<TaskSchedule> findDueSchedules(LocalDateTime now);

    Page<TaskSchedule> findByIsActive(Boolean isActive, Pageable pageable);

    long countByIsActive(Boolean isActive);
}
