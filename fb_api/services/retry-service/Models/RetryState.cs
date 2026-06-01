namespace RetryService.Models
{
    public class RetryState
    {
        public string? CommandId { get; set; }
        public int AttemptCount { get; set; }
        public DateTime NextRetryTime { get; set; }
        public string? LastError { get; set; }
        public string? FailedEventJson { get; set; }
        public string? Status { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime UpdatedAt { get; set; }
    }
}
