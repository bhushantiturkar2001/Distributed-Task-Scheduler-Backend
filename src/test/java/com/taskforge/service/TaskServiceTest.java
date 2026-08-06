package com.taskforge.service;

import com.taskforge.model.Task;
import com.taskforge.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@DisplayName("TaskService Unit Tests")
class TaskServiceTest {

    @Mock
    private TaskRepository taskRepository;

    @InjectMocks
    private TaskService taskService;

    private Task testTask;
    private UUID testTaskId;

    @BeforeEach
    void setUp() {
        testTaskId = UUID.randomUUID();
        testTask = Task.builder()
                .id(testTaskId)
                .name("Test Task")
                .description("Test Description")
                .taskType(Task.TaskType.HTTP_CALL)
                .priority(Task.TaskPriority.MEDIUM)
                .status(Task.TaskStatus.PENDING)
                .scheduledAt(LocalDateTime.now().plusHours(1))
                .retryCount(0)
                .maxRetries(3)
                .payload("{\"url\":\"https://api.example.com\"}")
                .build();
    }

    @Test
    @DisplayName("Should create task successfully")
    void testCreateTask_Success() {
        // Arrange
        when(taskRepository.save(any(Task.class))).thenReturn(testTask);

        // Act
        Task createdTask = taskService.createTask(testTask);

        // Assert
        assertNotNull(createdTask);
        assertEquals(Task.TaskStatus.PENDING, createdTask.getStatus());
        assertEquals(0, createdTask.getRetryCount());
        verify(taskRepository, times(1)).save(any(Task.class));
    }

    @Test
    @DisplayName("Should get task by id successfully")
    void testGetTaskById_Success() {
        // Arrange
        when(taskRepository.findById(testTaskId)).thenReturn(Optional.of(testTask));

        // Act
        Task foundTask = taskService.getTaskById(testTaskId);

        // Assert
        assertNotNull(foundTask);
        assertEquals(testTaskId, foundTask.getId());
        assertEquals("Test Task", foundTask.getName());
        verify(taskRepository, times(1)).findById(testTaskId);
    }

    @Test
    @DisplayName("Should throw exception when task not found by id")
    void testGetTaskById_NotFound() {
        // Arrange
        UUID nonExistentId = UUID.randomUUID();
        when(taskRepository.findById(nonExistentId)).thenReturn(Optional.empty());

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, 
            () -> taskService.getTaskById(nonExistentId));
        
        assertTrue(exception.getMessage().contains("Task not found"));
        verify(taskRepository, times(1)).findById(nonExistentId);
    }

    @Test
    @DisplayName("Should get all tasks with pagination")
    void testGetAllTasks_Success() {
        // Arrange
        Pageable pageable = PageRequest.of(0, 10);
        List<Task> taskList = List.of(testTask);
        Page<Task> taskPage = new PageImpl<>(taskList, pageable, 1);
        
        when(taskRepository.findAll(pageable)).thenReturn(taskPage);

        // Act
        Page<Task> result = taskService.getAllTasks(pageable);

        // Assert
        assertNotNull(result);
        assertEquals(1, result.getTotalElements());
        assertEquals(testTask, result.getContent().get(0));
        verify(taskRepository, times(1)).findAll(pageable);
    }

    @Test
    @DisplayName("Should get tasks by status with pagination")
    void testGetTasksByStatus_Success() {
        // Arrange
        Pageable pageable = PageRequest.of(0, 10);
        List<Task> taskList = List.of(testTask);
        Page<Task> taskPage = new PageImpl<>(taskList, pageable, 1);
        
        when(taskRepository.findByStatus(Task.TaskStatus.PENDING, pageable))
            .thenReturn(taskPage);

        // Act
        Page<Task> result = taskService.getTasksByStatus(Task.TaskStatus.PENDING, pageable);

        // Assert
        assertNotNull(result);
        assertEquals(1, result.getTotalElements());
        assertEquals(Task.TaskStatus.PENDING, result.getContent().get(0).getStatus());
        verify(taskRepository, times(1)).findByStatus(Task.TaskStatus.PENDING, pageable);
    }

    @Test
    @DisplayName("Should get due tasks")
    void testGetDueTasks_Success() {
        // Arrange
        LocalDateTime now = LocalDateTime.now();
        testTask.setScheduledAt(now.minusHours(1)); // Task is due
        List<Task> dueTasks = List.of(testTask);
        
        when(taskRepository.findDueTasks(now)).thenReturn(dueTasks);

        // Act
        List<Task> result = taskService.getDueTasks(now);

        // Assert
        assertNotNull(result);
        assertEquals(1, result.size());
        assertTrue(result.get(0).getScheduledAt().isBefore(now) || 
                   result.get(0).getScheduledAt().isEqual(now));
        verify(taskRepository, times(1)).findDueTasks(now);
    }

    @Test
    @DisplayName("Should get task count by status")
    void testGetTaskCountByStatus_Success() {
        // Arrange
        when(taskRepository.countByStatus(Task.TaskStatus.PENDING)).thenReturn(5L);

        // Act
        long count = taskService.getTaskCountByStatus(Task.TaskStatus.PENDING);

        // Assert
        assertEquals(5L, count);
        verify(taskRepository, times(1)).countByStatus(Task.TaskStatus.PENDING);
    }

    @Test
    @DisplayName("Should update task successfully when status is PENDING")
    void testUpdateTask_Success() {
        // Arrange
        Task updatedDetails = Task.builder()
                .name("Updated Task")
                .description("Updated Description")
                .taskType(Task.TaskType.LOG)
                .priority(Task.TaskPriority.HIGH)
                .scheduledAt(LocalDateTime.now().plusHours(2))
                .maxRetries(5)
                .payload("{\"message\":\"updated\"}")
                .build();

        when(taskRepository.findById(testTaskId)).thenReturn(Optional.of(testTask));
        when(taskRepository.save(any(Task.class))).thenReturn(testTask);

        // Act
        Task result = taskService.updateTask(testTaskId, updatedDetails);

        // Assert
        assertNotNull(result);
        verify(taskRepository, times(1)).findById(testTaskId);
        verify(taskRepository, times(1)).save(any(Task.class));
    }

    @Test
    @DisplayName("Should throw exception when updating non-PENDING task")
    void testUpdateTask_NotPendingStatus() {
        // Arrange
        testTask.setStatus(Task.TaskStatus.RUNNING);
        Task updatedDetails = Task.builder()
                .name("Updated Task")
                .build();

        when(taskRepository.findById(testTaskId)).thenReturn(Optional.of(testTask));

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, 
            () -> taskService.updateTask(testTaskId, updatedDetails));
        
        assertTrue(exception.getMessage().contains("Cannot update task that is not in PENDING status"));
        verify(taskRepository, times(1)).findById(testTaskId);
        verify(taskRepository, never()).save(any(Task.class));
    }

    @Test
    @DisplayName("Should delete task successfully when not RUNNING")
    void testDeleteTask_Success() {
        // Arrange
        when(taskRepository.findById(testTaskId)).thenReturn(Optional.of(testTask));
        doNothing().when(taskRepository).deleteById(testTaskId);

        // Act
        taskService.deleteTask(testTaskId);

        // Assert
        verify(taskRepository, times(1)).findById(testTaskId);
        verify(taskRepository, times(1)).deleteById(testTaskId);
    }

    @Test
    @DisplayName("Should throw exception when deleting RUNNING task")
    void testDeleteTask_RunningTask() {
        // Arrange
        testTask.setStatus(Task.TaskStatus.RUNNING);
        when(taskRepository.findById(testTaskId)).thenReturn(Optional.of(testTask));

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, 
            () -> taskService.deleteTask(testTaskId));
        
        assertTrue(exception.getMessage().contains("Cannot delete task that is currently running"));
        verify(taskRepository, times(1)).findById(testTaskId);
        verify(taskRepository, never()).deleteById(any());
    }

    @Test
    @DisplayName("Should cancel task successfully when valid status")
    void testCancelTask_Success() {
        // Arrange
        when(taskRepository.findById(testTaskId)).thenReturn(Optional.of(testTask));
        when(taskRepository.save(any(Task.class))).thenReturn(testTask);

        // Act
        taskService.cancelTask(testTaskId);

        // Assert
        verify(taskRepository, times(1)).findById(testTaskId);
        verify(taskRepository, times(1)).save(any(Task.class));
    }

    @Test
    @DisplayName("Should throw exception when cancelling RUNNING task")
    void testCancelTask_RunningTask() {
        // Arrange
        testTask.setStatus(Task.TaskStatus.RUNNING);
        when(taskRepository.findById(testTaskId)).thenReturn(Optional.of(testTask));

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, 
            () -> taskService.cancelTask(testTaskId));
        
        assertTrue(exception.getMessage().contains("Cannot cancel task in current status"));
        verify(taskRepository, times(1)).findById(testTaskId);
        verify(taskRepository, never()).save(any(Task.class));
    }

    @Test
    @DisplayName("Should throw exception when cancelling DEAD task")
    void testCancelTask_DeadTask() {
        // Arrange
        testTask.setStatus(Task.TaskStatus.DEAD);
        when(taskRepository.findById(testTaskId)).thenReturn(Optional.of(testTask));

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, 
            () -> taskService.cancelTask(testTaskId));
        
        assertTrue(exception.getMessage().contains("Cannot cancel task in current status"));
        verify(taskRepository, times(1)).findById(testTaskId);
        verify(taskRepository, never()).save(any(Task.class));
    }
}
