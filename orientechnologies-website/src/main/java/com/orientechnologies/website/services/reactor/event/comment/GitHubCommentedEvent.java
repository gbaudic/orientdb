package com.orientechnologies.website.services.reactor.event.comment;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.website.events.EventManager;
import com.orientechnologies.website.events.IssueCommentedEvent;
import com.orientechnologies.website.github.GComment;
import com.orientechnologies.website.github.GUser;
import com.orientechnologies.website.model.schema.OIssue;
import com.orientechnologies.website.model.schema.ORepository;
import com.orientechnologies.website.model.schema.dto.Comment;
import com.orientechnologies.website.model.schema.dto.Issue;
import com.orientechnologies.website.repository.CommentRepository;
import com.orientechnologies.website.repository.RepositoryRepository;
import com.orientechnologies.website.repository.UserRepository;
import com.orientechnologies.website.services.IssueService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by Enrico Risa on 14/11/14.
 */

@Component
public class GitHubCommentedEvent implements GithubCommentEvent {
  @Autowired
  private RepositoryRepository repositoryRepository;

  @Autowired
  private UserRepository       userRepository;

  @Autowired
  private CommentRepository    commentRepository;

  @Autowired
  private IssueService         issueService;

  @Autowired
  private EventManager         eventManager;

  @Override
  public void handle(String evt, ODocument payload) {
    ODocument commentDoc = payload.field("comment");
    ODocument issue = payload.field("issue");
    ODocument organization = payload.field("organization");
    ODocument repository = payload.field("repository");

    final GComment gComment = GComment.fromDoc(commentDoc);
    String repoName = repository.field(ORepository.NAME.toString());
    if (issue != null) {
      Integer issueNumber = issue.field(OIssue.NUMBER.toString());

      // sleep 2 sec. wait until the issue is created.
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      Issue issueDto = repositoryRepository.findIssueByRepoAndNumber(repoName, issueNumber);

      if (issueDto != null) {

        Comment comment = commentRepository.findByIssueAndCommentId(issueDto, gComment.getId());

        if (comment == null) {
          comment = new Comment();
          comment.setCommentId(gComment.getId());
          comment.setBody(gComment.getBody());
          GUser user = gComment.getUser();
          comment.setUser(userRepository.findUserOrCreateByLogin(user.getLogin(), user.getId()));
          comment.setCreatedAt(gComment.getCreatedAt());
          comment.setUpdatedAt(gComment.getUpdatedAt());
          comment = commentRepository.save(comment);
          issueService.commentIssue(issueDto, comment, false);
          comment.setOwner(issueDto);

          eventManager.pushInternalEvent(IssueCommentedEvent.EVENT, comment);
        }
      }
    }
  }

  @Override
  public String handleWhat() {
    return "created";
  }

  @Override
  public String formantPayload(ODocument payload) {
    return null;
  }
}
