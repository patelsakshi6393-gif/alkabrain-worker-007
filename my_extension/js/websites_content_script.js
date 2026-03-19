// When the page loads, if it comes from a source in Hunter products, there is
// a hash at the end with the email address to highlight. It will search it in
// this order:
// 1. Visible email address?
// 2. Email address in a mailto link?
// 3. Email address visible elsewhere in the code?
var PageContent, email;

PageContent = {
  getEmailInHash: function() {
    var email, hash;
    if (window.location.hash) {
      hash = window.location.hash;
      if (hash.indexOf(":") !== -1 && hash.split(":")[0]) {
        email = hash.split(":")[1];
        if (this.validateEmail(email)) {
          return email;
        } else {
          return false;
        }
      } else {
        return false;
      }
    } else {
      return false;
    }
  },
  validateEmail: function(email) {
    var re;
    re = /^(([^<>()\[\]\\.,;:\s@"]+(\.[^<>()\[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
    return re.test(email);
  },
  highlightEmail: function(email) {
    var containers;
    containers = this.getVisibleEmailContainers(email);
    if (containers.length > 0) {
      // We add a tag around the matching visible email addresses to highlight them
      $(containers).html($(containers[0]).html().replace(email, "<span class=\"hunter-email\">" + email + "</span>"));
      this.scrollToEmail();
      this.addLocationIcon();
      return this.displayMessage("found", email, containers.length);
    } else {
      // Next check: is it in a mailto address?
      return this.highlightMailto(email);
    }
  },
  highlightMailto: function(email) {
    if ($("a[href=\"mailto:" + email + "\"]:visible").length) {
      $("a[href=\"mailto:" + email + "\"]").addClass("hunter-email");
      this.scrollToEmail();
      this.addLocationIcon();
      return this.displayMessage("mailto", email, 1);
    } else {
      return this.searchCode(email);
    }
  },
  searchCode: function(email) {
    if ($("html").html().indexOf(email) !== -1) {
      return this.displayMessage("code", email, 0);
    } else {
      return this.displayMessage("notfound", email, 0);
    }
  },
  scrollToEmail: function() {
    return $("html, body").animate({
      scrollTop: $(".hunter-email:first").offset().top - 300
    }, 500);
  },
  addLocationIcon: function() {
    return setTimeout((function() {
      return $(".hunter-email").each(function(index) {
        var emailEl, emailHeight, emailWidth, position;
        emailEl = $(this);
        position = emailEl.offset();
        emailWidth = emailEl.outerWidth();
        emailHeight = emailEl.outerHeight();
        $("body").prepend("<img src=\"" + DOMPurify.sanitize(chrome.runtime.getURL("/img/location_icon.png")) + "\" alt=\"Here is the email found with Hunter!\" id=\"hunter-email-pointer\"/>");
        $("#hunter-email-pointer").css({
          "top": position.top - 63,
          "left": position.left + emailWidth / 2 - 25
        });
        return $("#hunter-email-pointer").fadeIn(300);
      });
    }), 1500);
  },
  displayMessage: function(message, email, count) {
    var src;
    src = chrome.runtime.getURL("/html/source_popup.html") + "?email=" + email + "&count=" + count + "&message=" + message;
    $("body").prepend("<iframe id='hunter-email-status' src='" + src + "'></iframe>");
    $("body").prepend("<div id='hunter-email-status-close'>&times;</div>");
    $("#hunter-email-status, #hunter-email-status-close").fadeIn(300);
    return $("#hunter-email-status-close").on("click", function() {
      return $("#hunter-email-status, #hunter-email-status-close, #hunter-email-pointer").fadeOut();
    });
  },
  getVisibleEmailContainers: function(email) {
    return $("body, body *").contents().filter(function() {
      return this.nodeType === 3 && this.nodeValue.indexOf(email) >= 0;
    }).parent(":visible");
  }
};

email = PageContent.getEmailInHash();

if (email) {
  PageContent.highlightEmail(email);
}
