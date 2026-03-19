var displayError, loadAccountInformation;

loadAccountInformation = function() {
  return Account.get(function(json) {
    if (json === "none") {
      $(".account-loading").hide();
      return $(".account-not-logged").show();
    } else {
      $(".account-loading").hide();
      $(".account-calls-used").text(Utilities.numberWithCommas(json.data.requests.searches.used));
      $(".account-calls-available").text(Utilities.numberWithCommas(json.data.requests.searches.available));
      $(".account-avatar__img").attr("src", "https://ui-avatars.com/api/?name=" + json.data.first_name + "+" + json.data.last_name + "&background=0F8DF4&color=fff&rounded=true");
      $(".account-avatar__img").attr("alt", json.data.first_name + " " + json.data.last_name);
      if (json.data.plan_level === 0) {
        $(".account-upgrade-cta").show();
      }
      return $(".account-logged").show();
    }
  });
};

displayError = function(html) {
  $("#error-message").html(html);
  $("html, body").animate({
    scrollTop: 0
  }, 300);
  return $("#error-message-container").delay(300).slideDown();
};

// Prepare what will be diplayed depending on the current page and domain
// - If it's not on a domain name, a default page explain how it works
// - If on LinkedIn, a page explains the feature is no longer available
// - Otherwise, the Domain Search is launched

chrome.tabs.query({
  active: true,
  currentWindow: true
}, function(tabs) {
  // We track the event
  Analytics.trackEvent("Open browser popup");
  // We do the localization for the text in the HTML
  $("[data-locale]").each(function() {
    return $(this).text(chrome.i18n.getMessage($(this).data("locale")));
  });
  $("[data-locale-title]").each(function() {
    return $(this).prop("title", chrome.i18n.getMessage($(this).data("localeTitle")));
  });
  return Account.getApiKey(function(api_key) {
    var currentDomain;
    // Get account information
    loadAccountInformation();
    chrome.storage.sync.get('current_leads_list_id', function(value) {
      window.current_leads_list_id = value.current_leads_list_id;
      return ListSelection.appendSelector();
    });
    window.api_key = api_key;
    window.url = tabs[0].url;
    currentDomain = new URL(tabs[0].url).hostname;
    // We clean the subdomains when relevant
    return Utilities.findRelevantDomain(currentDomain, function(domain) {
      var domainSearch;
      window.domain = domain;
      // We display a special message on LinkedIn
      if (window.domain === "linkedin.com") {
        $("#linkedin-notification").show();
        return $("#loading-placeholder").hide();
      // We display a soft 404 if there is no domain name
      } else if (window.domain === "" || window.domain.indexOf(".") === -1) {
        $("#empty-notification").show();
        return $("#loading-placeholder").hide();
      } else {
        // Launch the Domain Search
        domainSearch = new DomainSearch();
        return domainSearch.launch();
      }
    });
  });
});

var DomainSearch;

DomainSearch = function() {
  return {
    // Used to display full department names
    department_names: {
      executive: chrome.i18n.getMessage("department_executive"),
      it: chrome.i18n.getMessage("department_it"),
      finance: chrome.i18n.getMessage("department_finance"),
      management: chrome.i18n.getMessage("department_finance"),
      sales: chrome.i18n.getMessage("department_sales"),
      legal: chrome.i18n.getMessage("department_legal"),
      support: chrome.i18n.getMessage("department_support"),
      hr: chrome.i18n.getMessage("department_hr"),
      marketing: chrome.i18n.getMessage("department_marketing"),
      communication: chrome.i18n.getMessage("department_communication"),
      education: chrome.i18n.getMessage("department_education"),
      design: chrome.i18n.getMessage("department_design"),
      health: chrome.i18n.getMessage("department_health"),
      operations: chrome.i18n.getMessage("department_operations")
    },
    launch: function() {
      this.domain = window.domain;
      this.trial = typeof window.api_key === "undefined" || window.api_key === "";
      return this.fetch();
    },
    fetch: function() {
      var _this;
      _this = this;
      _this.cleanSearchResults();
      _this.department = _this.departmentFilter();
      _this.type = _this.typeFilter();
      if (_this.department || _this.type) {
        $('.filters__clear').show();
      } else {
        $('.filters__clear').hide();
      }
      return $.ajax({
        url: Api.domainSearch(_this.domain, _this.department, _this.type, window.api_key),
        headers: {
          "Email-Hunter-Origin": "chrome_extension"
        },
        type: "GET",
        data: {
          format: "json"
        },
        dataType: "json",
        jsonp: false,
        error: function(xhr) {
          $("#loading-placeholder").hide();
          $("#domain-search").show();
          $(".filters").css("visibility", "hidden");
          if (xhr.status === 400) {
            return displayError(chrome.i18n.getMessage("something_went_wrong_with_the_query"));
          } else if (xhr.status === 401) {
            return $(".connect-again-container").show();
          } else if (xhr.status === 403) {
            return $("#blocked-notification").show();
          } else if (xhr.status === 429) {
            if (!_this.trial) {
              return Account.returnRequestsError(function(e) {
                return displayError(e);
              });
            } else {
              return $(".connect-container").show();
            }
          } else {
            return displayError(DOMPurify.sanitize(xhr.responseJSON["errors"][0]["details"]));
          }
        },
        success: function(result) {
          _this.webmail = result.data.webmail;
          _this.pattern = result.data.pattern;
          _this.accept_all = result.data.accept_all;
          _this.verification = result.data.verification;
          _this.organization = result.data.organization;
          _this.results = result.data.emails;
          _this.results_count = result.meta.results;
          _this.offset = result.meta.offset;
          _this.type = result.meta.params.type;
          $("#loading-placeholder").hide();
          $("#domain-search").show();
          $(".filters").removeAttr("style");
          // Not logged in: we hide the Email Finder
          if (_this.trial) {
            $(".filters__by-name").hide();
            $(".find-by-name").hide();
          }
          _this.manageFilters();
          _this.clearFilters();
          return _this.render();
        }
      });
    },
    render: function() {
      var emailFinder, remaining_results, s;
      // Is webmail -> STOP
      if (this.webmail === true) {
        $("#domain-search").addClass("ds-no-results");
        $(".webmail-container .domain").text(this.domain);
        $(".webmail-container").show();
        return;
      }
      // No results -> STOP
      if (this.results_count === 0) {
        if (this.type || this.department) {
          $(".no-result-with-filters-container").show();
        } else {
          $("#domain-search").addClass("ds-no-results");
          $(".no-result-container .domain").text(this.domain);
          $(".no-result-container").show();
        }
        return;
      }
      // Display: the current domain
      $("#current-domain").text(this.domain);
      // Remove "no-results" class
      $("#domain-search").removeClass("ds-no-results");
      // Activate dropdown
      $("[data-toggle='dropdown']").dropdown();
      // Display: complete search link or Sign up CTA
      if (!this.trial) {
        $(".header-search-link").attr("href", "https://hunter.io/search/" + this.domain + "?utm_source=chrome_extension&utm_medium=chrome_extension&utm_campaign=extension&utm_content=browser_popup");
        $(".header-search-link").show();
      }
      // Display: the number of results
      if (this.results_count === 1) {
        s = "";
      } else {
        s = "s";
      }
      $("#domain-search .results-header__count").html(chrome.i18n.getMessage("results_for_domain", [DOMPurify.sanitize(Utilities.numberWithCommas(this.results_count)), s, DOMPurify.sanitize(this.domain)]));
      // Display: the email pattern if any
      if (this.pattern !== null) {
        $(".results-header__pattern").show();
        $(".results-header__pattern strong").html(this.addPatternTitle(this.pattern) + "@" + this.domain);
        $("[data-toggle='tooltip']").tooltip();
      }
      // Email Finder
      $(".ds-finder-form-company__name").text(this.organization);
      $(".ds-finder-form-company__logo").attr("src", "https://logo.clearbit.com/" + this.domain);
      emailFinder = new EmailFinder();
      emailFinder.validateForm();
      this.openFindbyName();
      // Display: the updated number of requests
      loadAccountInformation();
      // We count call to measure use
      countCall();
      this.feedbackNotification();
      // Display: the results
      this.showResults();
      // Render: set again an auto height on html
      $("html").css({
        height: "auto"
      });
      // Display: link to see more
      if (this.results_count > 10) {
        remaining_results = this.results_count - 10;
        return $(".search-results").append("<a class='see-more h-button h-button--sm' target='_blank' href='https://hunter.io/search/" + DOMPurify.sanitize(this.domain) + "?type=" + DOMPurify.sanitize(this.type) + "&department=" + DOMPurify.sanitize(this.department) + "&utm_source=chrome_extension&utm_medium=chrome_extension&utm_campaign=extension&utm_content=browser_popup'>" + chrome.i18n.getMessage("see_all_the_results", DOMPurify.sanitize(Utilities.numberWithCommas(remaining_results))) + "</a>");
      }
    },
    showResults: function() {
      var _this;
      _this = this;
      this.results.slice(0, 10).forEach(function(result) {
        var lead, result_tag, save_lead_button, template;
        // Sources
        if (result.sources.length === 1) {
          result.sources_link = "1 source";
        } else if (result.sources.length >= 20) {
          result.sources_link = "20+ sources";
        } else {
          result.sources_link = result.sources.length + " sources";
        }
        // Confidence score color
        if (result.confidence < 30) {
          result.confidence_score_class = "low-score";
        } else if (result.confidence >= 70) {
          result.confidence_score_class = "high-score";
        } else {
          result.confidence_score_class = "average-score";
        }
        // Save leads button
        if (!_this.trial) {
          result.lead_button = "<button class='ds-result__save h-button h-button--sm save-lead-button' type='button'>" + chrome.i18n.getMessage("save") + "</button>";
        } else {
          result.lead_button = "";
        }
        // Full name
        if (result.first_name !== null && result.last_name !== null) {
          result.full_name = DOMPurify.sanitize(result.first_name) + " " + DOMPurify.sanitize(result.last_name);
        }
        if (result.department) {
          result.department = DOMPurify.sanitize(_this.department_names[result.department]);
        }
        Handlebars.registerHelper("userDate", function(options) {
          return new Handlebars.SafeString(Utilities.dateInWords(options.fn(this)));
        });
        Handlebars.registerHelper("ifIsVerified", function(verification_status, options) {
          if (verification_status === "valid") {
            return options.fn(this);
          }
          return options.inverse(this);
        });
        Handlebars.registerHelper("ifIsAcceptAll", function(options) {
          if (_this.accept_all) {
            return options.fn(this);
          }
          return options.inverse(this);
        });
        // Integrate the result
        template = JST["src/browser_action/templates/search_results.hbs"];
        result_tag = $(Utilities.localizeHTML(template(result)));
        $(".search-results").append(result_tag);
        // Add the lead's data
        save_lead_button = result_tag.find(".save-lead-button");
        save_lead_button.data({
          email: result.value
        });
        lead = new LeadButton();
        lead.saveButtonListener(save_lead_button);
        lead.disableSaveLeadButtonIfLeadExists(save_lead_button);
        // Hide beautifully if the user is not logged
        if (_this.trial) {
          result_tag.find(".ds-result__email").removeClass("copy-email");
          result_tag.find(".ds-result__email").attr("title", chrome.i18n.getMessage("sign_up_to_uncover_more_emails"));
          return result_tag.find(".ds-result__email").html(result_tag.find(".ds-result__email").text().replace("**", "<span>aaa</span>"));
        }
      });
      this.openSources();
      $(".search-results").show();
      $("[data-toggle='tooltip']").tooltip();
      // For people not logged in, the copy and verification functions are not displayed
      if (_this.trial) {
        return $(".ds-result__verification").remove();
      } else {
        this.searchVerificationListener();
        return Utilities.copyEmailListener();
      }
    },
    searchVerificationListener: function() {
      var _this;
      _this = this;
      return $(".verification-link").unbind("click").click(function() {
        var email, verification_link_tag, verification_result_tag;
        verification_link_tag = $(this);
        verification_result_tag = $(this).parent().find(".verification-result");
        email = verification_link_tag.data("email");
        if (!email) {
          return;
        }
        verification_link_tag.html("<i class='far fa-spin fa-spinner-third'></i> " + chrome.i18n.getMessage("verifying") + "…</div>");
        verification_link_tag.attr("disabled", "true");
        // Launch the API call
        return $.ajax({
          url: Api.emailVerifier(email, window.api_key),
          headers: {
            "Email-Hunter-Origin": "chrome_extension"
          },
          type: "GET",
          data: {
            format: "json"
          },
          dataType: "json",
          jsonp: false,
          error: function(xhr, statusText, err) {
            verification_link_tag.removeAttr("disabled");
            verification_link_tag.show();
            if (xhr.status === 400) {
              return displayError(chrome.i18n.getMessage("something_went_wrong_with_the_query"));
            } else if (xhr.status === 401) {
              return $(".connect-again-container").show();
            } else if (xhr.status === 403) {
              $("#domain-search").hide();
              return $("#blocked-notification").show();
            } else if (xhr.status === 429) {
              if (!_this.trial) {
                return Account.returnRequestsError(function(e) {
                  return displayError(e);
                });
              } else {
                return $(".connect-container").show();
              }
            } else {
              return displayError(DOMPurify.sanitize(xhr.responseJSON["errors"][0]["details"]));
            }
          },
          success: function(result, statusText, xhr) {
            if (xhr.status === 202) {
              verification_link_tag.removeAttr("disabled");
              verification_link_tag.html("<span class='fa fa-exclamation-triangle'></span> " + chrome.i18n.getMessage("retry"));
              displayError(chrome.i18n.getMessage("email_verification_takes_longer"));
            } else if (xhr.status === 222) {
              verification_link_tag.removeAttr("disabled");
              verification_link_tag.html("<span class='fa fa-exclamation-triangle'></span> " + chrome.i18n.getMessage("retry"));
              displayError(DOMPurify.sanitize(result.errors[0].details));
              return;
            } else {
              verification_link_tag.remove();
              if (result.data.status === "valid") {
                verification_result_tag.html("<span class='tag tag--success' data-toggle='tooltip' data-placement='top' title='" + chrome.i18n.getMessage("valid") + "'> <span class='tag__label'> <i aria-hidden='true' class='tag__icon fas fa-shield-check'></i>" + result.data.score + "% </span> </span>");
              } else if (result.data.status === "invalid") {
                verification_result_tag.html("<span class='tag tag--danger' data-toggle='tooltip' data-placement='top' title='" + chrome.i18n.getMessage("invalid") + "'> <span class='tag__label'> <i aria-hidden='true' class='tag__icon fas fa-shield-xmark'></i>" + result.data.score + "% </span> </span>");
              } else if (result.data.status === "accept_all") {
                verification_result_tag.html("<span class='tag tag--warning' data-toggle='tooltip' data-placement='top' title='" + chrome.i18n.getMessage("accept_all") + "'> <span class='tag__label'> <i aria-hidden='true' class='tag__icon fas fa-shield-check'></i>" + result.data.score + "% </span> </span>");
              } else {
                verification_result_tag.html("<span class='tag' data-toggle='tooltip' data-placement='top' title='" + chrome.i18n.getMessage("unknown") + "'> <span class='tag__label'> <i aria-hidden='true' class='tag__icon fas fa-shield-slash'></i>" + result.data.score + "% </span> </span>");
              }
            }
            // We update the number of requests
            return loadAccountInformation();
          }
        });
      });
    },
    openSources: function() {
      return $(".sources-link").unbind("click").click(function(e) {
        if ($(this).parents(".ds-result").find(".ds-result__sources").is(":visible")) {
          $(this).parents(".ds-result").find(".ds-result__sources").slideUp(300);
          return $(this).find(".fa-angle-up").removeClass("fa-angle-up").addClass("fa-angle-down");
        } else {
          $(this).parents(".ds-result").find(".ds-result__sources").slideDown(300);
          return $(this).find(".fa-angle-down").removeClass("fa-angle-down").addClass("fa-angle-up");
        }
      });
    },
    openFindbyName: function() {
      return $(".filters__by-name").unbind("click").click(function(e) {
        if ($("#find-by-name").is(":visible")) {
          $(this).attr("aria-expanded", "false");
          return $("#find-by-name").hide();
        } else {
          $(this).attr("aria-expanded", "true");
          $("#find-by-name").show();
          return $("#full-name-field").focus();
        }
      });
    },
    manageFilters: function() {
      var _this;
      _this = this;
      $(document).on('click', '.dropdown .dropdown-menu', function(e) {
        return e.stopPropagation();
      });
      return $('.apply-filters').unbind().on("click", function() {
        var checked, checkedCount, dropdownContainer;
        checked = $(this).parent().find('[type="checkbox"]:checked, [type="radio"]:checked');
        checkedCount = checked.length;
        dropdownContainer = $(this).parents(".dropdown");
        dropdownContainer.removeClass("open");
        dropdownContainer.find('.h-button[data-toggle]').attr("aria-expanded", "false");
        if (checkedCount > 0) {
          dropdownContainer.find('.h-button[data-toggle]').attr("data-selected-filters", checkedCount);
        } else {
          dropdownContainer.find('.h-button[data-toggle]').removeAttr("data-selected-filters");
        }
        return _this.fetch();
      });
    },
    typeFilter: function() {
      var value;
      return value = $("#type-filters [type='radio']:checked").val();
    },
    departmentFilter: function() {
      var department;
      department = '';
      $("#departments-filters [type='checkbox']").each(function() {
        if ($(this).is(":checked")) {
          department = $(this).val();
          return false;
        }
      });
      return department;
    },
    clearFilters: function() {
      var _this;
      _this = this;
      return $(".clear-filters").unbind().on("click", function() {
        $('.filters').find('[type="checkbox"]:checked, [type="radio"]:checked').each(function() {
          return $(this).prop("checked", false);
        });
        $('.filters [data-selected-filters]').removeAttr("data-selected-filters");
        return _this.fetch();
      });
    },
    addPatternTitle: function(pattern) {
      pattern = pattern.replace("{first}", "<abbr data-toggle='tooltip' data-placement='top' title='" + chrome.i18n.getMessage("firts_name") + "'>{first}</abbr>").replace("{last}", "<abbr data-toggle='tooltip' data-placement='top' title='" + chrome.i18n.getMessage("last_name") + "'>{last}</abbr>").replace("{f}", "<abbr data-toggle='tooltip' data-placement='top' title='" + chrome.i18n.getMessage("first_name_initial") + "'>{f}</abbr>").replace("{l}", "<abbr data-toggle='tooltip' data-placement='top' title='" + chrome.i18n.getMessage("last_name_initial") + "'>{l}</abbr>");
      return pattern;
    },
    cleanSearchResults: function() {
      $("#loading-placeholder").show();
      $(".search-results").html("");
      return $(".no-result-with-filters-container").hide();
    },
    feedbackNotification: function() {
      chrome.storage.sync.get("calls_count", function(value) {
        if (value["calls_count"] >= 10) {
          return chrome.storage.sync.get("has_given_feedback", function(value) {
            if (typeof value["has_given_feedback"] === "undefined") {
              return $(".feedback-notification").slideDown(300);
            }
          });
        }
      });
      // Ask to note the extension
      $("#open-rate-notification").click(function() {
        $(".feedback-notification").slideUp(300);
        return $(".rate-notification").slideDown(300);
      });
      // Ask to give use feedback
      $("#open-contact-notification").click(function() {
        $(".feedback-notification").slideUp(300);
        return $(".contact-notification").slideDown(300);
      });
      return $(".feedback-link").click(function() {
        return chrome.storage.sync.set({
          "has_given_feedback": true
        });
      });
    }
  };
};

var EmailFinder;

EmailFinder = function() {
  return {
    full_name: this.full_name,
    domain: this.domain,
    email: this.email,
    score: this.score,
    sources: this.sources,
    validateForm: function() {
      var _this;
      _this = this;
      return $("#email-finder-search").unbind().submit(function() {
        if ($("#full-name-field").val().indexOf(" ") === -1 || $("#full-name-field").val().length <= 4) {
          $("#full-name-field").tooltip({
            title: chrome.i18n.getMessage("enter_full_name_to_find_email")
          }).tooltip("show");
        } else {
          _this.submit();
        }
        return false;
      });
    },
    submit: function() {
      $(".ds-finder-form__submit .far").removeClass("fa-search").addClass("fa-spinner-third fa-spin");
      this.domain = window.domain;
      this.full_name = $("#full-name-field").val();
      return this.fetch();
    },
    fetch: function() {
      var _this;
      this.cleanFinderResults();
      if (typeof $("#full-name-field").data("bs.tooltip") !== "undefined") {
        $("#full-name-field").tooltip("destroy");
      }
      _this = this;
      return $.ajax({
        url: Api.emailFinder(_this.domain, _this.full_name, window.api_key),
        headers: {
          "Email-Hunter-Origin": "chrome_extension"
        },
        type: "GET",
        data: {
          format: "json"
        },
        dataType: "json",
        jsonp: false,
        error: function(xhr, statusText, err) {
          $(".ds-finder-form__submit .far").removeClass("fa-spinner-third fa-spin").addClass("fa-search");
          if (xhr.status === 400) {
            return displayError(chrome.i18n.getMessage("something_went_wrong_with_the_query"));
          } else if (xhr.status === 401) {
            return $(".connect-again-container").show();
          } else if (xhr.status === 403) {
            $("#domain-search").hide();
            return $("#blocked-notification").show();
          } else if (xhr.status === 429) {
            if (!_this.trial) {
              return Account.returnRequestsError(function(e) {
                return displayError(e);
              });
            } else {
              return $(".connect-container").show();
            }
          } else {
            return displayError(DOMPurify.sanitize(xhr.responseJSON["errors"][0]["details"]));
          }
        },
        success: function(result) {
          if (result.data.email === null) {
            $(".ds-finder-form__submit .far").removeClass("fa-spinner-third fa-spin").addClass("fa-search");
            $(".no-finder-result .person-name").text(_this.full_name);
            return $(".no-finder-result").slideDown(200);
          } else {
            _this.domain = result.data.domain;
            _this.email = result.data.email;
            _this.score = result.data.score;
            _this.accept_all = result.data.accept_all;
            _this.verification = result.data.verification;
            _this.position = result.data.position;
            _this.company = result.data.company;
            _this.twitter = result.data.twitter;
            _this.linkedin = result.data.linkedin;
            _this.sources = result.data.sources;
            _this.first_name = Utilities.toTitleCase(result.data.first_name);
            _this.last_name = Utilities.toTitleCase(result.data.last_name);
            return _this.render();
          }
        }
      });
    },
    render: function() {
      var finder_html, lead, lead_button, s, template;
      $(".ds-finder-form__submit .far").removeClass("fa-spinner-third fa-spin").addClass("fa-search");
      // Display: complete search link or Sign up CTA
      if (!this.trial) {
        $(".header-search-link").attr("href", "https://hunter.io/search/" + this.domain + "?utm_source=chrome_extension&utm_medium=chrome_extension&utm_campaign=extension&utm_content=browser_popup");
        $(".header-search-link").show();
      }
      // Confidence score color
      if (this.score < 30) {
        this.confidence_score_class = "low-score";
      } else if (this.score > 70) {
        this.confidence_score_class = "high-score";
      } else {
        this.confidence_score_class = "average-score";
      }
      // Display: the method used
      if (this.sources.length > 0) {
        s = this.sources.length === 1 ? "" : "s";
        this.method = chrome.i18n.getMessage("we_found_this_email_on_the_web", [this.sources.length, s]);
      } else {
        this.method = "This email address is our best guess for this person. We haven't found it on the web.";
      }
      // Prepare the template
      Handlebars.registerHelper("ifIsVerified", function(verification_status, options) {
        if (verification_status === "valid") {
          return options.fn(this);
        }
        return options.inverse(this);
      });
      Handlebars.registerHelper("md5", function(options) {
        return new Handlebars.SafeString(Utilities.MD5(options.fn(this)));
      });
      template = JST["src/browser_action/templates/finder.hbs"];
      finder_html = $(Utilities.localizeHTML(template(this)));
      // Generate the DOM with the template and display it
      $("#email-finder").html(finder_html);
      $("#email-finder").slideDown(200);
      // Display: the sources if any
      if (this.sources.length > 0) {
        $(".ds-result__sources").show();
      }
      // Display: the tooltips
      $("[data-toggle='tooltip']").tooltip();
      // Event: the copy action
      Utilities.copyEmailListener();
      // Display: the button to save the lead
      lead_button = $(".ds-result--single .save-lead-button");
      lead_button.data({
        first_name: this.first_name,
        last_name: this.last_name,
        email: this.email,
        confidence_score: this.score
      });
      lead = new LeadButton();
      lead.saveButtonListener(lead_button);
      return lead.disableSaveLeadButtonIfLeadExists(lead_button);
    },
    cleanFinderResults: function() {
      $("#email-finder").html("");
      $("#email-finder").slideUp(200);
      return $(".no-finder-result").slideUp(200);
    }
  };
};

var LeadButton;

LeadButton = function() {
  return {
    first_name: this.first_name,
    last_name: this.last_name,
    position: this.position,
    email: this.email,
    company: this.organization,
    website: window.domain,
    source: "Hunter (Domain Search)",
    phone_number: this.phone_number,
    linkedin_url: this.linkedin,
    twitter: this.twitter,
    saveButtonListener: function(selector) {
      var _this;
      _this = this;
      return $(selector).unbind("click").click(function() {
        var attributes, lead, lead_button;
        lead_button = $(this);
        lead = lead_button.data();
        lead_button.prop("disabled", true);
        lead_button.html("<span class='far fa-spin fa-spinner-third' aria-label='" + chrome.i18n.getMessage("loading") + "'></span>");
        attributes = ["first_name", "last_name", "position", "email", "company", "website", "source", "phone_number", "linkedin_url", "twitter", "email"];
        lead = {};
        attributes.forEach(function(attribute) {
          if (_this[attribute] === void 0) {
            return lead[attribute] = lead_button.data(attribute);
          } else {
            return lead[attribute] = _this[attribute];
          }
        });
        if (window.current_leads_list_id) {
          lead["leads_list_id"] = window.current_leads_list_id;
        }
        return _this.save(lead, lead_button);
      });
    },
    save: function(lead, button) {
      return $.ajax({
        url: Api.leads(api_key),
        headers: {
          "Email-Hunter-Origin": "chrome_extension"
        },
        type: "POST",
        data: lead,
        dataType: "json",
        jsonp: false,
        error: function(xhr, statusText, err) {
          button.replaceWith("<span class='tag tag--danger'><span class='tag__label'><i aria-hidden='true' class='tag__icon far fa-times'></i> " + chrome.i18n.getMessage("failed") + "</span></span>");
          displayError(DOMPurify.sanitize(xhr.responseJSON["errors"][0]["details"]));
          if (xhr.status === 422) {
            return window.current_leads_list_id = void 0;
          }
        },
        success: function(response) {
          return button.replaceWith("<span class='tag tag--success'><span class='tag__label'><i aria-hidden='true' class='tag__icon far fa-check'></i> " + chrome.i18n.getMessage("saved") + "</span></span>");
        }
      });
    },
    disableSaveLeadButtonIfLeadExists: function(selector) {
      return $(selector).each(function() {
        var lead, lead_button;
        lead_button = $(this);
        lead = $(this).data();
        return $.ajax({
          url: Api.leadsExist(lead.email, window.api_key),
          headers: {
            "Email-Hunter-Origin": "chrome_extension"
          },
          type: "GET",
          data: {
            format: "json"
          },
          dataType: "json",
          jsonp: false,
          success: function(response) {
            if (response.data.id !== null) {
              return lead_button.replaceWith("<span class='tag tag--success'><span class='tag__label'><i aria-hidden='true' class='tag__icon far fa-check'></i> " + chrome.i18n.getMessage("saved") + "</span></span>");
            }
          }
        });
      });
    }
  };
};

var ListSelection;

ListSelection = {
  appendSelector: function() {
    var _this;
    _this = this;
    return _this.getLeadsLists(function(json) {
      var selected_list_id;
      if (json !== "none") {
        $(".list-select-container").append("<select id='leads-list' class='leads-manager__list h-select h-select--sm'></select>");
        // We determine the selected list
        if (window.current_leads_list_id) {
          selected_list_id = window.current_leads_list_id;
        } else {
          selected_list_id = json.data.leads_lists[0].id;
        }
        // We add all the lists in the select field
        json.data.leads_lists.forEach(function(val, i) {
          var selected;
          if (parseInt(selected_list_id) === parseInt(val.id)) {
            selected = "selected='selected'";
          } else {
            selected = "";
          }
          return $(".leads-manager__list").append("<option " + selected + " value='" + val.id + "'>" + val.name + "</option>");
        });
        // We add a link to the current list
        $(".leads-manager__link").attr("href", "https://hunter.io/leads?leads_list_id=" + selected_list_id + "&utm_source=chrome_extension&utm_medium=chrome_extension&utm_campaign=extension&utm_content=browser_popup");
        $(".leads-manager__list").append("<option value='new_list'>" + chrome.i18n.getMessage("create_a_new_list") + "...</option>");
        return _this.updateCurrent();
      }
    });
  },
  updateCurrent: function() {
    return $(".leads-manager__list").on("change", function() {
      if ($(this).val() === "new_list") {
        return Utilities.openInNewTab("https://hunter.io/leads-lists/new?utm_source=chrome_extension&utm_medium=chrome_extension&utm_campaign=extension");
      } else {
        chrome.storage.sync.set({
          "current_leads_list_id": $(this).val()
        });
        window.current_leads_list_id = $(this).val();
        return $(".leads-manager__link").attr("href", "https://hunter.io/leads?leads_list_id=" + $(this).val() + "&utm_source=chrome_extension&utm_medium=chrome_extension&utm_campaign=extension&utm_content=browser_popup");
      }
    });
  },
  getLeadsLists: function(callback) {
    return Account.getApiKey(function(api_key) {
      if (api_key !== "") {
        return $.ajax({
          url: Api.leadsList(window.api_key),
          headers: {
            "Email-Hunter-Origin": "chrome_extension"
          },
          type: "GET",
          dataType: "json",
          jsonp: false,
          success: function(json) {
            return callback(json);
          }
        });
      } else {
        return callback("none");
      }
    });
  }
};
