using FacebookAPI___PageAPI.models;
using FacebookAPI___PageAPI.services;
using Microsoft.AspNetCore.Mvc;

namespace FacebookAPI___PageAPI.controllers
{
    [ApiController]
    [Route("api/page")]
    public class PageController : ControllerBase
    {
        private readonly FacebookService _facebookService;

        public PageController(FacebookService facebookService)
        {
            _facebookService = facebookService;
        }

        [HttpGet("{pageId}")]
        public async Task<IActionResult> GetPageInfo(string pageId)
        {
            try
            {
                var result = await _facebookService.GetPageInfoAsync(pageId);
                return Ok(new ApiResponse
                {
                    Success = true,
                    Message = "Lấy thông tin page thành công",
                    Data = result
                });
            }
            catch (Exception ex)
            {
                return StatusCode(503, new ApiResponse
                {
                    Success = false,
                    Message = "Facebook API tạm thời không khả dụng",
                    Error = ex.Message
                });
            }
        }

        [HttpGet("{pageId}/posts")]
        public async Task<IActionResult> GetPagePosts(string pageId)
        {
            try
            {
                var result = await _facebookService.GetPagePostsAsync(pageId);
                return Ok(new ApiResponse
                {
                    Success = true,
                    Message = "Lấy danh sách bài viết thành công",
                    Data = result
                });
            }
            catch (Exception ex)
            {
                return StatusCode(503, new ApiResponse
                {
                    Success = false,
                    Message = "Facebook API tạm thời không khả dụng",
                    Error = ex.Message
                });
            }
        }

        [HttpPost("{pageId}/posts")]
        public async Task<IActionResult> CreatePost(string pageId, [FromBody] CreatePostRequest request)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(request.Message))
                {
                    return BadRequest(new ApiResponse
                    {
                        Success = false,
                        Message = "Message không được để trống"
                    });
                }

                var result = await _facebookService.CreatePostAsync(pageId, request.Message);
                return Ok(new ApiResponse
                {
                    Success = true,
                    Message = "Đăng bài thành công",
                    Data = result
                });
            }
            catch (Exception ex)
            {
                return StatusCode(503, new ApiResponse
                {
                    Success = false,
                    Message = "Facebook API tạm thời không khả dụng",
                    Error = ex.Message
                });
            }
        }

        [HttpDelete("post/{postId}")]
        public async Task<IActionResult> DeletePost(string postId)
        {
            try
            {
                var result = await _facebookService.DeletePostAsync(postId);
                return Ok(new ApiResponse
                {
                    Success = true,
                    Message = "Xóa bài viết thành công",
                    Data = result
                });
            }
            catch (Exception ex)
            {
                return StatusCode(503, new ApiResponse
                {
                    Success = false,
                    Message = "Facebook API tạm thời không khả dụng",
                    Error = ex.Message
                });
            }
        }

        [HttpGet("post/{postId}/comments")]
        public async Task<IActionResult> GetComments(string postId)
        {
            try
            {
                var result = await _facebookService.GetPostCommentsAsync(postId);
                return Ok(new ApiResponse
                {
                    Success = true,
                    Message = "Lấy comments thành công",
                    Data = result
                });
            }
            catch (Exception ex)
            {
                return StatusCode(503, new ApiResponse
                {
                    Success = false,
                    Message = "Facebook API tạm thời không khả dụng",
                    Error = ex.Message
                });
            }
        }

        [HttpGet("post/{postId}/likes")]
        public async Task<IActionResult> GetLikes(string postId)
        {
            try
            {
                var result = await _facebookService.GetPostLikesAsync(postId);
                return Ok(new ApiResponse
                {
                    Success = true,
                    Message = "Lấy likes thành công",
                    Data = result
                });
            }
            catch (Exception ex)
            {
                return StatusCode(503, new ApiResponse
                {
                    Success = false,
                    Message = "Facebook API tạm thời không khả dụng",
                    Error = ex.Message
                });
            }
        }

        [HttpGet("{pageId}/insights")]
        public async Task<IActionResult> GetInsights(string pageId)
        {
            try
            {
                var result = await _facebookService.GetPageInsightsAsync(pageId);
                return Ok(new ApiResponse
                {
                    Success = true,
                    Message = "Lấy insights thành công",
                    Data = result
                });
            }
            catch (Exception ex)
            {
                return BadRequest(new ApiResponse
                {
                    Success = false,
                    Message = "Lỗi khi lấy insights",
                    Error = ex.Message
                });
            }
        }

        [HttpPost("comment/{commentId}/reply")]
        public async Task<IActionResult> ReplyComment(string commentId, [FromBody] ReplyCommentRequest request)
        {
            try
            {
                if (string.IsNullOrWhiteSpace(request.Message))
                {
                    return BadRequest(new ApiResponse
                    {
                        Success = false,
                        Message = "Nội dung phản hồi không được để trống"
                    });
                }

                var result = await _facebookService.ReplyToCommentAsync(commentId, request.Message);
                return Ok(new ApiResponse
                {
                    Success = true,
                    Message = "Phản hồi bình luận thành công",
                    Data = result
                });
            }
            catch (Exception ex)
            {
                return StatusCode(503, new ApiResponse
                {
                    Success = false,
                    Message = "Facebook API tạm thời không khả dụng",
                    Error = ex.Message
                });
            }
        }

        [HttpPost("comment/{commentId}/hide")]
        public async Task<IActionResult> HideComment(string commentId)
        {
            try
            {
                var result = await _facebookService.HideCommentAsync(commentId);
                return Ok(new ApiResponse
                {
                    Success = true,
                    Message = "Ẩn bình luận thành công",
                    Data = result
                });
            }
            catch (Exception ex)
            {
                return StatusCode(503, new ApiResponse
                {
                    Success = false,
                    Message = "Facebook API tạm thời không khả dụng",
                    Error = ex.Message
                });
            }
        }
    }
}
