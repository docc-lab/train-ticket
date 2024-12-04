package travel2.exception;

/**
 * Custom exception class for service-related errors
 */
public class ServiceException extends RuntimeException {
    
    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new ServiceException with the specified error message
     * 
     * @param message the error message
     */
    public ServiceException(String message) {
        super(message);
    }

    /**
     * Constructs a new ServiceException with the specified error message and cause
     * 
     * @param message the error message
     * @param cause the cause of the exception
     */
    public ServiceException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new ServiceException with the specified cause
     * 
     * @param cause the cause of the exception
     */
    public ServiceException(Throwable cause) {
        super(cause);
    }
}
